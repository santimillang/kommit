use kafka_protocol::ResponseError;
use kafka_protocol::messages::ApiVersionsResponse;
use kafka_protocol::messages::api_versions_response::ApiVersion;

use crate::api::SUPPORTED;

pub fn handle() -> ApiVersionsResponse {
    ApiVersionsResponse::default().with_api_keys(
        SUPPORTED
            .iter()
            .map(|(key, min, max)| {
                ApiVersion::default()
                    .with_api_key(*key as i16)
                    .with_min_version(*min)
                    .with_max_version(*max)
            })
            .collect(),
    )
}

/// Answer to an ApiVersions request newer than we speak: always encoded as v0.
pub fn unsupported_version() -> ApiVersionsResponse {
    handle().with_error_code(ResponseError::UnsupportedVersion.code())
}
