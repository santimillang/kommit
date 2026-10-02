pub mod api_versions;
pub mod create_topics;
pub mod metadata;
pub mod produce;
pub mod records;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind, ResponseKind};

use crate::broker::Broker;
use crate::log::LogError;

/// Every API kommit answers, with the version range it implements. ApiVersions advertises exactly this.
pub const SUPPORTED: &[(ApiKey, i16, i16)] = &[
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::Produce, 3, 12),
];

pub fn supported_range(key: ApiKey) -> Option<(i16, i16)> {
    SUPPORTED
        .iter()
        .find(|(k, _, _)| *k == key)
        .map(|(_, min, max)| (*min, *max))
}

pub fn log_error_code(e: &LogError) -> i16 {
    match e {
        LogError::OutOfRange(_) => ResponseError::OffsetOutOfRange.code(),
        LogError::Storage(_) | LogError::Faulted(_) => ResponseError::KafkaStorageError.code(),
    }
}

/// Returns `None` when the request gets no response (acks=0 produce).
pub async fn dispatch(
    broker: &Broker,
    header: &RequestHeader,
    request: RequestKind,
) -> Option<ResponseKind> {
    match request {
        RequestKind::ApiVersions(_) => Some(ResponseKind::ApiVersions(api_versions::handle())),
        RequestKind::Metadata(r) => Some(ResponseKind::Metadata(metadata::handle(broker, r).await)),
        RequestKind::CreateTopics(r) => Some(ResponseKind::CreateTopics(
            create_topics::handle(broker, r).await,
        )),
        RequestKind::Produce(r) => {
            // The client id becomes the Git author of every record it produces.
            let client_id = header.client_id.as_deref().unwrap_or("");
            produce::handle(broker, client_id, r)
                .await
                .map(ResponseKind::Produce)
        }
        // The server only dispatches APIs listed in SUPPORTED.
        _ => None,
    }
}
