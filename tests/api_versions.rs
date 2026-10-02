mod common;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, ResponseHeader};
use kafka_protocol::protocol::{Decodable, StrBytes};

#[tokio::test]
async fn api_versions_lists_only_what_kommit_implements() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut client = common::TestClient::connect(addr).await;
    let req = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("kommit-test"))
        .with_client_software_version(StrBytes::from_static_str("1"));
    let resp = client.send(3, req).await;
    assert_eq!(resp.error_code, 0);
    let api_versions = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::ApiVersions as i16)
        .unwrap();
    assert_eq!((api_versions.min_version, api_versions.max_version), (0, 3));
    assert_eq!(resp.api_keys.len(), kommit::api::SUPPORTED.len());
}

#[tokio::test]
async fn too_new_api_versions_gets_a_v0_unsupported_version_answer() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut client = common::TestClient::connect(addr).await;
    // ApiVersions v99 does not exist; a future client would send the flexible (v2) request
    // header. The body is irrelevant because the broker must not parse it.
    let mut resp = client
        .send_raw(ApiKey::ApiVersions as i16, 99, 2, &[])
        .await
        .unwrap();
    ResponseHeader::decode(&mut resp, 0).unwrap();
    let body = ApiVersionsResponse::decode(&mut resp, 0).unwrap();
    assert_eq!(body.error_code, ResponseError::UnsupportedVersion.code());
    assert!(!body.api_keys.is_empty());
}

#[tokio::test]
async fn unimplemented_api_closes_the_connection() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut client = common::TestClient::connect(addr).await;
    // DescribeAcls (29) is a real API kommit does not implement.
    assert!(client.send_raw(29, 1, 1, &[]).await.is_none());
}
