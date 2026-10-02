pub mod api_versions;

use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind, ResponseKind};

use crate::broker::Broker;

/// Every API kommit answers, with the version range it implements. ApiVersions advertises exactly this.
pub const SUPPORTED: &[(ApiKey, i16, i16)] = &[(ApiKey::ApiVersions, 0, 3)];

pub fn supported_range(key: ApiKey) -> Option<(i16, i16)> {
    SUPPORTED
        .iter()
        .find(|(k, _, _)| *k == key)
        .map(|(_, min, max)| (*min, *max))
}

/// Returns `None` when the request gets no response (acks=0 produce).
pub async fn dispatch(
    broker: &Broker,
    header: &RequestHeader,
    request: RequestKind,
) -> Option<ResponseKind> {
    let _ = (broker, header);
    match request {
        RequestKind::ApiVersions(_) => Some(ResponseKind::ApiVersions(api_versions::handle())),
        // The server only dispatches APIs listed in SUPPORTED.
        _ => None,
    }
}
