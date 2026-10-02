mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{DescribeGroupsRequest, GroupId, ListGroupsRequest};
use kafka_protocol::protocol::StrBytes;
use kommit::broker::Broker;
use kommit::git::store::GitStore;
use kommit::groups::coordinator::JoinRequest;
use kommit::storage::GitStorage;

async fn stable_single_member_group(broker: &Broker) -> String {
    let joined = broker
        .coordinator
        .join(JoinRequest {
            group: "g".into(),
            member_id: String::new(),
            client_id: "app".into(),
            client_host: "/127.0.0.1".into(),
            session_timeout: Duration::from_secs(30),
            rebalance_timeout: Duration::from_secs(30),
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
        })
        .await
        .unwrap();
    broker
        .coordinator
        .sync(
            "g",
            joined.generation,
            &joined.member_id,
            vec![(joined.member_id.clone(), Bytes::from_static(b"assigned"))],
        )
        .await
        .unwrap();
    joined.member_id
}

#[tokio::test]
async fn list_and_describe_show_live_groups() {
    let (addr, broker) = common::start_mem_broker().await;
    let member = stable_single_member_group(&broker).await;
    let mut c = common::TestClient::connect(addr).await;

    let listed = c.send(5, ListGroupsRequest::default()).await;
    assert_eq!(listed.error_code, 0);
    let g = &listed.groups[0];
    assert_eq!(
        (
            &*g.group_id.0,
            &*g.protocol_type,
            &*g.group_state,
            &*g.group_type
        ),
        ("g", "consumer", "Stable", "classic")
    );
    // older versions have no state field and must still encode
    assert_eq!(
        c.send(0, ListGroupsRequest::default()).await.groups.len(),
        1
    );

    let described = c
        .send(
            5,
            DescribeGroupsRequest::default().with_groups(vec![
                GroupId(StrBytes::from_static_str("g")),
                GroupId(StrBytes::from_static_str("nope")),
            ]),
        )
        .await;
    let g = &described.groups[0];
    assert_eq!((g.error_code, &*g.group_state), (0, "Stable"));
    assert_eq!(
        (&*g.protocol_type, &*g.protocol_data),
        ("consumer", "range")
    );
    assert_eq!(g.members.len(), 1);
    let m = &g.members[0];
    assert_eq!(&*m.member_id, member.as_str());
    assert_eq!((&*m.client_id, &*m.client_host), ("app", "/127.0.0.1"));
    assert_eq!(
        (m.member_metadata.as_ref(), m.member_assignment.as_ref()),
        (&b"meta"[..], &b"assigned"[..])
    );
    let unknown = &described.groups[1];
    assert_eq!((unknown.error_code, &*unknown.group_state), (0, "Dead"));
}

#[tokio::test]
async fn groups_with_offsets_are_listed_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d.git");
    {
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(
            kommit::config::Config::for_tests(),
            Arc::new(GitStorage::new(store)),
        )
        .await
        .unwrap();
        let t = broker.create_topic("orders", 1).await.unwrap();
        t.partitions[0].commit_offset("billing", 0).await.unwrap();
    }
    let store = Arc::new(GitStore::open_or_init(&path).unwrap());
    let (addr, _broker) = common::start_broker(Arc::new(GitStorage::new(store))).await;
    let mut c = common::TestClient::connect(addr).await;
    let listed = c.send(4, ListGroupsRequest::default()).await;
    let g = &listed.groups[0];
    assert_eq!((&*g.group_id.0, &*g.group_state), ("billing", "Empty"));
}
