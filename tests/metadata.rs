mod common;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::BrokerId;

#[tokio::test]
async fn metadata_advertises_this_broker_and_leads_every_partition() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 2).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    let resp = c.send(12, common::metadata_req(None, false)).await;
    assert_eq!(resp.brokers.len(), 1);
    assert_eq!(resp.brokers[0].node_id, BrokerId(0));
    assert_eq!(resp.brokers[0].port, addr.port() as i32);
    assert_eq!(resp.controller_id, BrokerId(0));
    let t = &resp.topics[0];
    assert_eq!(&*t.name.as_ref().unwrap().0, "orders");
    assert_eq!(t.partitions.len(), 2);
    assert!(
        t.partitions
            .iter()
            .all(|p| p.leader_id == BrokerId(0) && p.isr_nodes == vec![BrokerId(0)])
    );
}

#[tokio::test]
async fn named_metadata_auto_creates_only_when_asked() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut c = common::TestClient::connect(addr).await;
    let resp = c
        .send(12, common::metadata_req(Some(&["fresh"]), false))
        .await;
    assert_eq!(
        resp.topics[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    let resp = c
        .send(12, common::metadata_req(Some(&["fresh"]), true))
        .await;
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].partitions.len(), 1);
    let resp = c
        .send(12, common::metadata_req(Some(&["a..b"]), true))
        .await;
    assert_eq!(
        resp.topics[0].error_code,
        ResponseError::InvalidTopicException.code()
    );
}

#[tokio::test]
async fn create_topics_creates_validates_and_rejects() {
    let (addr, broker) = common::start_mem_broker().await;
    let mut c = common::TestClient::connect(addr).await;
    let resp = c.send(7, common::create_topic_req("orders", 3)).await;
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].num_partitions, 3);
    assert_eq!(broker.topic("orders").await.unwrap().partitions.len(), 3);

    let resp = c.send(7, common::create_topic_req("orders", 3)).await;
    assert_eq!(
        resp.topics[0].error_code,
        ResponseError::TopicAlreadyExists.code()
    );
    let resp = c.send(7, common::create_topic_req(".hidden", 1)).await;
    assert_eq!(
        resp.topics[0].error_code,
        ResponseError::InvalidTopicException.code()
    );
    let resp = c.send(7, common::create_topic_req("zero", 0)).await;
    assert_eq!(
        resp.topics[0].error_code,
        ResponseError::InvalidPartitions.code()
    );

    let resp = c.send(7, common::create_topic_req("defaulted", -1)).await;
    assert_eq!(resp.topics[0].num_partitions, 1);

    let dry = common::create_topic_req("dry", 2).with_validate_only(true);
    let resp = c.send(7, dry).await;
    assert_eq!(resp.topics[0].error_code, 0);
    assert!(broker.topic("dry").await.is_none());
}
