mod common;

use kafka_protocol::ResponseError;
use kommit::api::records::encode_batch;
use kommit::record::Record;

fn batch(values: &[&str]) -> bytes::Bytes {
    let recs: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, v)| (i as i64, Record::text(1000 + i as i64, v)))
        .collect();
    encode_batch(&recs).unwrap()
}

#[tokio::test]
async fn produce_appends_and_returns_base_offsets() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let r = c
        .send(9, common::produce_req("orders", 0, batch(&["a", "b"]), 1))
        .await;
    let p = &r.responses[0].partition_responses[0];
    assert_eq!((p.error_code, p.base_offset), (0, 0));
    let r = c
        .send(9, common::produce_req("orders", 0, batch(&["c"]), -1))
        .await;
    assert_eq!(r.responses[0].partition_responses[0].base_offset, 2);
    let log = broker.topic("orders").await.unwrap().partitions[0].clone();
    assert_eq!(log.high_watermark(), 3);
}

#[tokio::test]
async fn produce_to_unknown_topic_or_partition_is_rejected() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let r = c
        .send(9, common::produce_req("nope", 0, batch(&["a"]), 1))
        .await;
    assert_eq!(
        r.responses[0].partition_responses[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    let r = c
        .send(9, common::produce_req("orders", 5, batch(&["a"]), 1))
        .await;
    assert_eq!(
        r.responses[0].partition_responses[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    assert!(
        broker.topic("nope").await.is_none(),
        "produce must not auto-create"
    );
}

#[tokio::test]
async fn corrupt_batches_are_refused() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let mut bad = batch(&["a"]).to_vec();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    let r = c
        .send(9, common::produce_req("orders", 0, bad.into(), 1))
        .await;
    assert_eq!(
        r.responses[0].partition_responses[0].error_code,
        ResponseError::CorruptMessage.code()
    );
}

#[tokio::test]
async fn acks_zero_gets_no_response_but_still_writes() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    c.send_no_reply(
        9,
        common::produce_req("orders", 0, batch(&["fire", "forget"]), 0),
    )
    .await;
    // the next request's response proves nothing was sent for the acks=0 one
    let r = c
        .send(9, common::produce_req("orders", 0, batch(&["acked"]), 1))
        .await;
    assert_eq!(r.responses[0].partition_responses[0].base_offset, 2);
}
