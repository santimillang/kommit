mod common;

use kafka_protocol::ResponseError;
use kommit::record::Record;

async fn offsets(c: &mut common::TestClient, topic: &str, ts: i64) -> (i16, i64, i64) {
    let r = c.send(6, common::list_offsets_req(topic, 0, ts)).await;
    let p = &r.topics[0].partitions[0];
    (p.error_code, p.offset, p.timestamp)
}

#[tokio::test]
async fn earliest_latest_and_by_timestamp() {
    let (addr, broker) = common::start_mem_broker().await;
    let t = broker.create_topic("orders", 1).await.unwrap();
    t.partitions[0]
        .append(
            "p",
            vec![
                Record::text(100, "a"),
                Record::text(300, "b"),
                Record::text(200, "c"),
            ],
        )
        .await
        .unwrap();
    let mut c = common::TestClient::connect(addr).await;
    assert_eq!(offsets(&mut c, "orders", -2).await, (0, 0, -1));
    assert_eq!(offsets(&mut c, "orders", -1).await, (0, 3, -1));
    assert_eq!(offsets(&mut c, "orders", 150).await, (0, 1, 300));
    assert_eq!(offsets(&mut c, "orders", 999).await, (0, -1, -1));
    assert_eq!(
        offsets(&mut c, "orders", -7).await.0,
        ResponseError::InvalidRequest.code()
    );
    assert_eq!(
        offsets(&mut c, "nope", -1).await.0,
        ResponseError::UnknownTopicOrPartition.code()
    );
}
