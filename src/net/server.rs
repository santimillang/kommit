use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Encodable, decode_request_header_from_buffer};
use tokio::net::{TcpListener, TcpStream};

use crate::api;
use crate::broker::Broker;
use crate::net::frame::{read_frame, write_frame};

pub async fn serve(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let broker = broker.clone();
        // One task per connection: a panic in a handler drops this connection only.
        tokio::spawn(async move {
            let client_host = format!("/{}", peer.ip());
            if let Err(e) = handle_connection(stream, broker, &client_host).await {
                tracing::debug!(%peer, "connection closed: {e:#}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    broker: Arc<Broker>,
    client_host: &str,
) -> Result<()> {
    stream.set_nodelay(true)?;
    // Requests on one connection are handled in order, like a Kafka broker's muted channel.
    while let Some(frame) = read_frame(&mut stream).await? {
        if let Some(response) = handle_frame(&broker, frame, client_host).await? {
            write_frame(&mut stream, &response).await?;
        }
    }
    Ok(())
}

/// Handles one request frame. `Ok(None)` means no response is due; `Err` closes the connection.
pub async fn handle_frame(
    broker: &Broker,
    mut frame: Bytes,
    client_host: &str,
) -> Result<Option<Bytes>> {
    let header = decode_request_header_from_buffer(&mut frame)?;
    let api_key =
        ApiKey::try_from(header.request_api_key).map_err(|_| anyhow!("unknown API key"))?;
    let version = header.request_api_version;
    let Some((min, max)) = api::supported_range(api_key) else {
        bail!("{api_key:?} is not implemented");
    };
    if !(min..=max).contains(&version) {
        if api_key == ApiKey::ApiVersions {
            let body = ResponseKind::ApiVersions(api::api_versions::unsupported_version());
            return Ok(Some(encode_response(
                header.correlation_id,
                api_key,
                0,
                &body,
            )?));
        }
        bail!("{api_key:?} v{version} is not supported");
    }
    let request = RequestKind::decode(api_key, &mut frame, version)?;
    let ctx = api::RequestContext {
        client_id: header.client_id.as_deref().unwrap_or("").to_string(),
        client_host: client_host.to_string(),
        version,
    };
    let Some(response) = api::dispatch(broker, &ctx, request).await else {
        return Ok(None);
    };
    Ok(Some(encode_response(
        header.correlation_id,
        api_key,
        version,
        &response,
    )?))
}

fn encode_response(
    correlation_id: i32,
    api_key: ApiKey,
    version: i16,
    body: &ResponseKind,
) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut buf, api_key.response_header_version(version))?;
    body.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
