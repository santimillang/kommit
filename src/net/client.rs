//! A minimal Kafka client: enough for the `kommit` CLI to talk to a running broker.

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::net::TcpStream;

use crate::net::frame::{read_frame, write_frame};

pub struct Client {
    stream: TcpStream,
    correlation_id: i32,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connecting to {addr}"))?;
        Ok(Client {
            stream,
            correlation_id: 0,
        })
    }

    pub async fn send<R: Request>(&mut self, version: i16, req: R) -> Result<R::Response> {
        self.correlation_id += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("kommit-cli")));
        let mut buf = BytesMut::new();
        header
            .encode(&mut buf, R::header_version(version))
            .map_err(|e| anyhow!("encoding request header: {e:#}"))?;
        req.encode(&mut buf, version)
            .map_err(|e| anyhow!("encoding api {} v{version}: {e:#}", R::KEY))?;
        write_frame(&mut self.stream, &buf).await?;
        let mut resp = read_frame(&mut self.stream)
            .await?
            .context("the broker closed the connection")?;
        let rh = ResponseHeader::decode(
            &mut resp,
            <R::Response as HeaderVersion>::header_version(version),
        )
        .map_err(|e| anyhow!("decoding response header: {e:#}"))?;
        if rh.correlation_id != self.correlation_id {
            bail!(
                "response for request {} arrived out of order",
                rh.correlation_id
            );
        }
        R::Response::decode(&mut resp, version)
            .map_err(|e| anyhow!("decoding api {} v{version} response: {e:#}", R::KEY))
    }
}
