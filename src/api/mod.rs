pub mod api_versions;
pub mod create_topics;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod join_group;
pub mod leave_group;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_fetch;
pub mod produce;
pub mod records;
pub mod sync_group;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use crate::broker::Broker;
use crate::log::LogError;

/// Every API kommit answers, with the version range it implements. ApiVersions advertises exactly this.
pub const SUPPORTED: &[(ApiKey, i16, i16)] = &[
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::Produce, 3, 12),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 6),
    (ApiKey::FindCoordinator, 0, 6),
    (ApiKey::OffsetCommit, 2, 9),
    (ApiKey::OffsetFetch, 1, 9),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
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

/// Who sent a request, and at which API version.
pub struct RequestContext {
    pub client_id: String,
    /// The peer address as Kafka's tools show it, e.g. `/127.0.0.1`.
    pub client_host: String,
    pub version: i16,
}

/// Returns `None` when the request gets no response (acks=0 produce).
pub async fn dispatch(
    broker: &Broker,
    ctx: &RequestContext,
    request: RequestKind,
) -> Option<ResponseKind> {
    match request {
        RequestKind::ApiVersions(_) => Some(ResponseKind::ApiVersions(api_versions::handle())),
        RequestKind::Metadata(r) => Some(ResponseKind::Metadata(metadata::handle(broker, r).await)),
        RequestKind::CreateTopics(r) => Some(ResponseKind::CreateTopics(
            create_topics::handle(broker, r).await,
        )),
        // The client id becomes the Git author of every record it produces.
        RequestKind::Produce(r) => produce::handle(broker, &ctx.client_id, r)
            .await
            .map(ResponseKind::Produce),
        RequestKind::Fetch(r) => Some(ResponseKind::Fetch(fetch::handle(broker, r).await)),
        RequestKind::ListOffsets(r) => Some(ResponseKind::ListOffsets(
            list_offsets::handle(broker, r).await,
        )),
        RequestKind::FindCoordinator(r) => Some(ResponseKind::FindCoordinator(
            find_coordinator::handle(broker, ctx, r),
        )),
        RequestKind::OffsetCommit(r) => Some(ResponseKind::OffsetCommit(
            offset_commit::handle(broker, r).await,
        )),
        RequestKind::OffsetFetch(r) => Some(ResponseKind::OffsetFetch(
            offset_fetch::handle(broker, ctx, r).await,
        )),
        RequestKind::JoinGroup(r) => Some(ResponseKind::JoinGroup(
            join_group::handle(broker, ctx, r).await,
        )),
        RequestKind::SyncGroup(r) => Some(ResponseKind::SyncGroup(
            sync_group::handle(broker, ctx, r).await,
        )),
        RequestKind::Heartbeat(r) => Some(ResponseKind::Heartbeat(heartbeat::handle(broker, r))),
        RequestKind::LeaveGroup(r) => Some(ResponseKind::LeaveGroup(leave_group::handle(
            broker, ctx, r,
        ))),
        // The server only dispatches APIs listed in SUPPORTED.
        _ => None,
    }
}
