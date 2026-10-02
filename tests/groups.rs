mod common;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::{BrokerId, GroupId, OffsetCommitRequest, OffsetFetchRequest};
use kafka_protocol::protocol::StrBytes;
use kommit::record::Record;

fn group(g: &str) -> GroupId {
    GroupId(StrBytes::from_string(g.to_string()))
}

fn commit_req(
    g: &str,
    generation: i32,
    member: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(group(g))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(StrBytes::from_string(member.to_string()))
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(common::topic_name(topic))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(partition)
                        .with_committed_offset(offset),
                ]),
        ])
}

fn fetch_v7(g: &str, topics: Option<&[(&str, &[i32])]>) -> OffsetFetchRequest {
    OffsetFetchRequest::default()
        .with_group_id(group(g))
        .with_topics(topics.map(|ts| {
            ts.iter()
                .map(|(t, ps)| {
                    OffsetFetchRequestTopic::default()
                        .with_name(common::topic_name(t))
                        .with_partition_indexes(ps.to_vec())
                })
                .collect()
        }))
}

async fn broker_with_orders() -> (std::net::SocketAddr, std::sync::Arc<kommit::broker::Broker>) {
    let (addr, broker) = common::start_mem_broker().await;
    let t = broker.create_topic("orders", 2).await.unwrap();
    let recs = (0..3).map(|i| Record::text(i, "x")).collect();
    t.partitions[0].append("p", recs).await.unwrap();
    (addr, broker)
}

#[tokio::test]
async fn find_coordinator_points_at_this_broker() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut c = common::TestClient::connect(addr).await;
    let r = c
        .send(
            3,
            FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("g")),
        )
        .await;
    assert_eq!(
        (r.error_code, r.node_id, r.port),
        (0, BrokerId(0), addr.port() as i32)
    );
    let r = c
        .send(
            4,
            FindCoordinatorRequest::default().with_coordinator_keys(vec![
                StrBytes::from_static_str("a"),
                StrBytes::from_static_str("b"),
            ]),
        )
        .await;
    assert_eq!(r.coordinators.len(), 2);
    assert!(
        r.coordinators
            .iter()
            .all(|k| k.error_code == 0 && k.port == addr.port() as i32)
    );
    // transactions are not supported
    let r = c
        .send(
            3,
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("tx"))
                .with_key_type(1),
        )
        .await;
    assert_eq!(r.error_code, ResponseError::CoordinatorNotAvailable.code());
}

#[tokio::test]
async fn offsets_commit_and_fetch_round_trip() {
    let (addr, _broker) = broker_with_orders().await;
    let mut c = common::TestClient::connect(addr).await;
    let r = c.send(8, commit_req("g", -1, "", "orders", 0, 2)).await;
    assert_eq!(r.topics[0].partitions[0].error_code, 0);

    let r = c.send(7, fetch_v7("g", Some(&[("orders", &[0, 1])]))).await;
    let parts = &r.topics[0].partitions;
    assert_eq!((parts[0].committed_offset, parts[0].error_code), (2, 0));
    assert_eq!((parts[1].committed_offset, parts[1].error_code), (-1, 0));

    // all topics: only partitions with a committed offset
    let r = c.send(7, fetch_v7("g", None)).await;
    assert_eq!(r.topics.len(), 1);
    assert_eq!(r.topics[0].partitions.len(), 1);
    assert_eq!(r.topics[0].partitions[0].committed_offset, 2);

    // v8+ batches groups
    let req = OffsetFetchRequest::default().with_groups(vec![
        OffsetFetchRequestGroup::default()
            .with_group_id(group("g"))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(common::topic_name("orders"))
                    .with_partition_indexes(vec![0]),
            ])),
    ]);
    let r = c.send(8, req).await;
    assert_eq!(r.groups[0].topics[0].partitions[0].committed_offset, 2);
}

#[tokio::test]
async fn bad_commits_get_kafka_errors() {
    let (addr, broker) = broker_with_orders().await;
    let mut c = common::TestClient::connect(addr).await;
    let r = c.send(8, commit_req("g", -1, "", "nope", 0, 0)).await;
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    let r = c.send(8, commit_req("g", -1, "", "orders", 0, 99)).await;
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        ResponseError::OffsetOutOfRange.code()
    );
    // a live group rejects commits from strangers
    let join = broker
        .coordinator
        .join(kommit::groups::coordinator::JoinRequest {
            group: "live".into(),
            member_id: String::new(),
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            session_timeout: std::time::Duration::from_secs(10),
            rebalance_timeout: std::time::Duration::from_secs(10),
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), bytes::Bytes::new())],
        })
        .await
        .unwrap();
    let r = c
        .send(
            8,
            commit_req("live", join.generation, "ghost", "orders", 0, 1),
        )
        .await;
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        ResponseError::UnknownMemberId.code()
    );
    let r = c
        .send(
            8,
            commit_req("live", join.generation + 1, &join.member_id, "orders", 0, 1),
        )
        .await;
    assert_eq!(
        r.topics[0].partitions[0].error_code,
        ResponseError::IllegalGeneration.code()
    );
}
