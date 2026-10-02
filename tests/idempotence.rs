mod common;

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::{InitProducerIdRequest, ProducerId, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

/// One idempotent batch from producer `pid` with consecutive sequences starting at `first_seq`.
fn idempotent_batch(pid: i64, first_seq: i32, values: &[&str]) -> Bytes {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: pid,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: first_seq + i as i32,
            timestamp: 1000,
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    buf.freeze()
}

#[tokio::test]
async fn init_producer_id_hands_out_fresh_ids() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut c = common::TestClient::connect(addr).await;
    let a = c
        .send(
            4,
            InitProducerIdRequest::default().with_transaction_timeout_ms(60_000),
        )
        .await;
    let b = c
        .send(
            4,
            InitProducerIdRequest::default().with_transaction_timeout_ms(60_000),
        )
        .await;
    assert_eq!((a.error_code, a.producer_epoch), (0, 0));
    assert!(a.producer_id.0 >= 0);
    assert_ne!(a.producer_id, b.producer_id);

    let tx = c
        .send(
            4,
            InitProducerIdRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("tx"))))
                .with_transaction_timeout_ms(60_000),
        )
        .await;
    assert_eq!(tx.error_code, ResponseError::UnsupportedVersion.code());
    assert_eq!(tx.producer_id, ProducerId(-1));
}

#[tokio::test]
async fn retried_batches_are_stored_once_and_gaps_are_refused() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let send = |batch| common::produce_req("orders", 0, batch, -1);

    let first = c.send(9, send(idempotent_batch(7, 0, &["a", "b"]))).await;
    let retry = c.send(9, send(idempotent_batch(7, 0, &["a", "b"]))).await;
    let p = |r: &kafka_protocol::messages::ProduceResponse| {
        let p = &r.responses[0].partition_responses[0];
        (p.error_code, p.base_offset)
    };
    assert_eq!(p(&first), (0, 0));
    assert_eq!(
        p(&retry),
        (0, 0),
        "a retried batch must not be appended again"
    );
    let log = broker.topic("orders").await.unwrap().partitions[0].clone();
    assert_eq!(log.high_watermark(), 2);

    let next = c.send(9, send(idempotent_batch(7, 2, &["c"]))).await;
    assert_eq!(p(&next), (0, 2));
    let gap = c.send(9, send(idempotent_batch(7, 5, &["z"]))).await;
    assert_eq!(p(&gap).0, ResponseError::OutOfOrderSequenceNumber.code());
    assert_eq!(log.high_watermark(), 3);

    // a different producer has its own sequence space
    let other = c.send(9, send(idempotent_batch(8, 0, &["x"]))).await;
    assert_eq!(p(&other), (0, 3));
}
