//! A minimal Kafka client: enough for the `kommit` CLI to talk to a running broker.

use anyhow::{Context, Result, anyhow, bail};
use bytes::BytesMut;
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use std::time::Duration;

use tokio::net::TcpStream;

use crate::net::frame::{read_frame, write_frame};

/// Long enough for a fork of a big topic, short enough that a dead broker is noticed.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Client {
    stream: TcpStream,
    correlation_id: i32,
    timeout: Duration,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = tokio::time::timeout(DEFAULT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("timed out connecting to {addr}"))?
            .with_context(|| format!("connecting to {addr}"))?;
        Ok(Client {
            stream,
            correlation_id: 0,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// How long a request may wait for its response.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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
        let exchange = async {
            write_frame(&mut self.stream, &buf).await?;
            read_frame(&mut self.stream).await
        };
        let mut resp = tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| anyhow!("timed out after {:?} waiting for the broker", self.timeout))??
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
