//! Every API must answer at every version kommit advertises. Real clients negotiate down
//! (kcat 1.6 / librdkafka 1.8 uses ListOffsets v2, Metadata v4, Produce v7), and a response
//! field set outside its version range makes the broker drop the connection.

mod common;

use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::{ApiVersionsRequest, MetadataRequest};
use kommit::api::records::encode_batch;
use kommit::api::supported_range;
use kommit::record::Record;

fn range(key: ApiKey) -> std::ops::RangeInclusive<i16> {
    let (min, max) = supported_range(key).unwrap();
    min..=max
}

#[tokio::test]
async fn every_api_answers_at_every_advertised_version() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;

    for v in range(ApiKey::ApiVersions) {
        let r = c.send(v, ApiVersionsRequest::default()).await;
        assert_eq!(r.error_code, 0, "ApiVersions v{v}");
    }
    for v in range(ApiKey::Metadata) {
        // allow_auto_topic_creation only exists from v4; leave it at its default
        let r = c
            .send(v, MetadataRequest::default().with_topics(None))
            .await;
        assert_eq!(r.topics.len(), 1, "Metadata v{v}");
    }
    for v in range(ApiKey::CreateTopics) {
        let r = c
            .send(v, common::create_topic_req(&format!("t-v{v}"), 1))
            .await;
        assert_eq!(r.topics[0].error_code, 0, "CreateTopics v{v}");
    }
    for v in range(ApiKey::Produce) {
        let batch = encode_batch(&[(0, Record::text(1, "x"))]).unwrap();
        let r = c.send(v, common::produce_req("orders", 0, batch, 1)).await;
        assert_eq!(
            r.responses[0].partition_responses[0].error_code, 0,
            "Produce v{v}"
        );
    }
    for v in range(ApiKey::Fetch) {
        let r = c
            .send(v, common::fetch_req("orders", 0, 0, 0, 1 << 20))
            .await;
        assert_eq!(r.responses[0].partitions[0].error_code, 0, "Fetch v{v}");
    }
    for v in range(ApiKey::ListOffsets) {
        let r = c.send(v, common::list_offsets_req("orders", 0, -2)).await;
        assert_eq!(r.topics[0].partitions[0].error_code, 0, "ListOffsets v{v}");
    }
}
