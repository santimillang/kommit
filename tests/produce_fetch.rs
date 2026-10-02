mod common;

use kafka_protocol::ResponseError;
use kommit::api::records::{decode_batches, encode_batch};
use kommit::record::Record;
use std::time::{Duration, Instant};

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

#[tokio::test]
async fn fetch_returns_records_with_their_offsets() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    c.send(
        9,
        common::produce_req("orders", 0, batch(&["a", "b", "c"]), 1),
    )
    .await;
    let r = c
        .send(12, common::fetch_req("orders", 0, 1, 0, 1 << 20))
        .await;
    let p = &r.responses[0].partitions[0];
    assert_eq!((p.error_code, p.high_watermark), (0, 3));
    let got = decode_batches(p.records.clone().unwrap()).unwrap();
    let values: Vec<_> = got
        .iter()
        .map(|(o, r)| (*o, r.value.clone().unwrap()))
        .collect();
    assert_eq!(values, vec![(1, "b".into()), (2, "c".into())]);
}

#[tokio::test]
async fn a_record_bigger_than_partition_max_bytes_is_still_returned() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    c.send(
        9,
        common::produce_req("orders", 0, batch(&["a fairly long value", "b"]), 1),
    )
    .await;
    let r = c.send(12, common::fetch_req("orders", 0, 0, 0, 1)).await;
    let got = decode_batches(r.responses[0].partitions[0].records.clone().unwrap()).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, 0);
}

#[tokio::test]
async fn fetch_errors_for_unknown_partitions_and_bad_offsets() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let r = c.send(12, common::fetch_req("nope", 0, 0, 0, 1024)).await;
    assert_eq!(
        r.responses[0].partitions[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    let started = Instant::now();
    let r = c
        .send(12, common::fetch_req("orders", 0, 5, 5_000, 1024))
        .await;
    assert_eq!(
        r.responses[0].partitions[0].error_code,
        ResponseError::OffsetOutOfRange.code()
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "errors must not wait out max_wait_ms"
    );
}

#[tokio::test]
async fn empty_fetch_waits_for_max_wait_then_returns_nothing() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let started = Instant::now();
    let r = c
        .send(12, common::fetch_req("orders", 0, 0, 200, 1024))
        .await;
    assert!(started.elapsed() >= Duration::from_millis(180));
    let p = &r.responses[0].partitions[0];
    assert_eq!(p.error_code, 0);
    assert!(p.records.as_ref().is_none_or(|b| b.is_empty()));
}

#[tokio::test]
async fn long_poll_wakes_up_when_a_record_arrives() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let waiter = tokio::spawn(async move {
        let mut c = common::TestClient::connect(addr).await;
        c.send(12, common::fetch_req("orders", 0, 0, 10_000, 1 << 20))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    let mut producer = common::TestClient::connect(addr).await;
    producer
        .send(9, common::produce_req("orders", 0, batch(&["wake"]), 1))
        .await;
    let r = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    let got = decode_batches(r.responses[0].partitions[0].records.clone().unwrap()).unwrap();
    assert_eq!(got[0].1.value.as_deref(), Some(&b"wake"[..]));
}
