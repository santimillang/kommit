#![allow(dead_code)] // each test binary uses a different subset of the harness

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    CreateTopicsRequest, MetadataRequest, ProduceRequest, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kommit::broker::Broker;
use kommit::config::Config;
use kommit::net::frame::{read_frame, write_frame};
use kommit::storage::{MemStorage, Storage};
use tokio::net::{TcpListener, TcpStream};

/// Starts a broker on an ephemeral port; the handle lets a test stop the accept loop.
pub async fn start_broker_with_handle(
    storage: Arc<dyn Storage>,
) -> (
    SocketAddr,
    Arc<Broker>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = Config::for_tests();
    config.advertised_port = addr.port() as i32;
    let broker = Broker::start(config, storage).await.unwrap();
    let handle = tokio::spawn(kommit::net::server::serve(listener, broker.clone()));
    (addr, broker, handle)
}

pub async fn start_broker(storage: Arc<dyn Storage>) -> (SocketAddr, Arc<Broker>) {
    let (addr, broker, _detached) = start_broker_with_handle(storage).await;
    (addr, broker)
}

pub async fn start_mem_broker() -> (SocketAddr, Arc<Broker>) {
    start_broker(Arc::new(MemStorage)).await
}

pub fn topic_name(s: &str) -> TopicName {
    TopicName(StrBytes::from_string(s.to_string()))
}

pub fn metadata_req(topics: Option<&[&str]>, auto_create: bool) -> MetadataRequest {
    MetadataRequest::default()
        .with_topics(topics.map(|ts| {
            ts.iter()
                .map(|t| MetadataRequestTopic::default().with_name(Some(topic_name(t))))
                .collect()
        }))
        .with_allow_auto_topic_creation(auto_create)
}

pub fn create_topic_req(name: &str, partitions: i32) -> CreateTopicsRequest {
    CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(name))
                .with_num_partitions(partitions)
                .with_replication_factor(-1),
        ])
}

pub fn produce_req(topic: &str, partition: i32, batch: Bytes, acks: i16) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(5000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name(topic))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(batch)),
                ]),
        ])
}

pub struct TestClient {
    stream: TcpStream,
    correlation_id: i32,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> Self {
        TestClient {
            stream: TcpStream::connect(addr).await.unwrap(),
            correlation_id: 0,
        }
    }

    fn header(&mut self, api_key: i16, version: i16) -> RequestHeader {
        self.correlation_id += 1;
        RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("kommit-test")))
    }

    fn encode<R: Request>(&mut self, version: i16, req: R) -> BytesMut {
        let header = self.header(R::KEY, version);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, R::header_version(version)).unwrap();
        req.encode(&mut buf, version).unwrap();
        buf
    }

    pub async fn send<R: Request>(&mut self, version: i16, req: R) -> R::Response {
        let buf = self.encode(version, req);
        write_frame(&mut self.stream, &buf).await.unwrap();
        let mut resp = read_frame(&mut self.stream)
            .await
            .unwrap()
            .expect("broker closed the connection");
        let rh = ResponseHeader::decode(
            &mut resp,
            <R::Response as HeaderVersion>::header_version(version),
        )
        .unwrap();
        assert_eq!(rh.correlation_id, self.correlation_id);
        R::Response::decode(&mut resp, version).unwrap()
    }

    /// Sends a request and does not wait for a reply (acks=0 produce).
    pub async fn send_no_reply<R: Request>(&mut self, version: i16, req: R) {
        let buf = self.encode(version, req);
        write_frame(&mut self.stream, &buf).await.unwrap();
    }

    /// Sends a hand-built request with the given request header version; returns the raw
    /// response frame, or None if the broker closed the connection.
    pub async fn send_raw(
        &mut self,
        api_key: i16,
        version: i16,
        header_version: i16,
        body: &[u8],
    ) -> Option<Bytes> {
        let header = self.header(api_key, version);
        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        buf.extend_from_slice(body);
        write_frame(&mut self.stream, &buf).await.unwrap();
        read_frame(&mut self.stream).await.ok().flatten()
    }
}
