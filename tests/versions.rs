//! Every API must answer at every version kommit advertises. Real clients negotiate down
//! (kcat 1.6 / librdkafka 1.8 uses ListOffsets v2, Metadata v4, Produce v7), and a response
//! field set outside its version range makes the broker drop the connection.

mod common;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiVersionsRequest, DescribeGroupsRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest,
    InitProducerIdRequest, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    ListGroupsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest,
};
use kafka_protocol::protocol::StrBytes;
use kommit::api::records::encode_batch;
use kommit::api::supported_range;
use kommit::record::Record;

fn range(key: ApiKey) -> std::ops::RangeInclusive<i16> {
    let (min, max) = supported_range(key).unwrap();
    min..=max
}

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

/// A JoinGroup carrying only the fields version `v` has.
fn join_req(v: i16, group: &str) -> JoinGroupRequest {
    let req = JoinGroupRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_session_timeout_ms(30_000)
        .with_protocol_type(s("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(s("range"))
                .with_metadata(Bytes::from_static(b"m")),
        ]);
    if v >= 1 {
        req.with_rebalance_timeout_ms(30_000)
    } else {
        req
    }
}

/// A fresh single-member group, joined at the newest version.
async fn joined(c: &mut common::TestClient, group: &str) -> JoinGroupResponse {
    let r = c.send(9, join_req(9, group)).await;
    assert_eq!(r.error_code, 0, "join {group}");
    r
}

/// kafka-protocol's ApiKey ranges can run ahead of what its request types decode
/// (InitProducerId v6 did), so every advertised range must fit the request type itself.
#[test]
fn advertised_versions_are_decodable() {
    use kafka_protocol::messages::*;
    use kafka_protocol::protocol::{Message, VersionRange};
    let decodable: Vec<(ApiKey, VersionRange)> = vec![
        (ApiKey::ApiVersions, ApiVersionsRequest::VERSIONS),
        (ApiKey::Metadata, MetadataRequest::VERSIONS),
        (ApiKey::CreateTopics, CreateTopicsRequest::VERSIONS),
        (ApiKey::Produce, ProduceRequest::VERSIONS),
        (ApiKey::Fetch, FetchRequest::VERSIONS),
        (ApiKey::ListOffsets, ListOffsetsRequest::VERSIONS),
        (ApiKey::FindCoordinator, FindCoordinatorRequest::VERSIONS),
        (ApiKey::OffsetCommit, OffsetCommitRequest::VERSIONS),
        (ApiKey::OffsetFetch, OffsetFetchRequest::VERSIONS),
        (ApiKey::JoinGroup, JoinGroupRequest::VERSIONS),
        (ApiKey::SyncGroup, SyncGroupRequest::VERSIONS),
        (ApiKey::Heartbeat, HeartbeatRequest::VERSIONS),
        (ApiKey::LeaveGroup, LeaveGroupRequest::VERSIONS),
        (ApiKey::ListGroups, ListGroupsRequest::VERSIONS),
        (ApiKey::DescribeGroups, DescribeGroupsRequest::VERSIONS),
        (ApiKey::InitProducerId, InitProducerIdRequest::VERSIONS),
    ];
    assert_eq!(
        decodable.len(),
        kommit::api::SUPPORTED.len(),
        "update this list"
    );
    for (key, limits) in decodable {
        let (min, max) = supported_range(key).unwrap();
        assert!(
            min >= limits.min && max <= limits.max,
            "{key:?} advertises v{min}-{max}, request decodes v{}-{}",
            limits.min,
            limits.max
        );
    }
}

#[tokio::test]
async fn group_and_producer_apis_answer_at_every_advertised_version() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;

    for v in range(ApiKey::FindCoordinator) {
        let req = if v < 4 {
            FindCoordinatorRequest::default().with_key(s("g"))
        } else {
            FindCoordinatorRequest::default().with_coordinator_keys(vec![s("g")])
        };
        let r = c.send(v, req).await;
        let code = if v < 4 {
            r.error_code
        } else {
            r.coordinators[0].error_code
        };
        assert_eq!(code, 0, "FindCoordinator v{v}");
    }
    for v in range(ApiKey::JoinGroup) {
        let r = c.send(v, join_req(v, &format!("join-v{v}"))).await;
        assert_eq!(r.error_code, 0, "JoinGroup v{v}");
        // error path: unknown member id
        let r = c
            .send(
                v,
                join_req(v, &format!("join-v{v}")).with_member_id(s("ghost")),
            )
            .await;
        assert_eq!(
            r.error_code,
            ResponseError::UnknownMemberId.code(),
            "JoinGroup v{v} error"
        );
    }
    for v in range(ApiKey::SyncGroup) {
        let group = format!("sync-v{v}");
        let j = joined(&mut c, &group).await;
        let mut req = SyncGroupRequest::default()
            .with_group_id(GroupId(s(&group)))
            .with_generation_id(j.generation_id)
            .with_member_id(j.member_id.clone())
            .with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(j.member_id.clone())
                    .with_assignment(Bytes::from_static(b"a")),
            ]);
        if v >= 5 {
            req = req
                .with_protocol_type(Some(s("consumer")))
                .with_protocol_name(Some(s("range")));
        }
        let r = c.send(v, req).await;
        assert_eq!(
            (r.error_code, r.assignment.as_ref()),
            (0, &b"a"[..]),
            "SyncGroup v{v}"
        );
        // error path: wrong generation
        let mut bad = SyncGroupRequest::default()
            .with_group_id(GroupId(s(&group)))
            .with_generation_id(j.generation_id + 5)
            .with_member_id(j.member_id.clone());
        if v >= 5 {
            bad = bad
                .with_protocol_type(Some(s("consumer")))
                .with_protocol_name(Some(s("range")));
        }
        let r = c.send(v, bad).await;
        assert_eq!(
            r.error_code,
            ResponseError::IllegalGeneration.code(),
            "SyncGroup v{v} error"
        );
    }
    let j = joined(&mut c, "hb").await;
    for v in range(ApiKey::Heartbeat) {
        let ok = HeartbeatRequest::default()
            .with_group_id(GroupId(s("hb")))
            .with_generation_id(j.generation_id)
            .with_member_id(j.member_id.clone());
        // CompletingRebalance accepts heartbeats; there is no sync in this loop.
        assert_eq!(c.send(v, ok.clone()).await.error_code, 0, "Heartbeat v{v}");
        let ghost = ok.with_member_id(s("ghost"));
        assert_eq!(
            c.send(v, ghost).await.error_code,
            ResponseError::UnknownMemberId.code(),
            "Heartbeat v{v} error"
        );
    }
    for v in range(ApiKey::LeaveGroup) {
        let group = format!("leave-v{v}");
        let j = joined(&mut c, &group).await;
        let req = LeaveGroupRequest::default().with_group_id(GroupId(s(&group)));
        let req = if v < 3 {
            req.with_member_id(j.member_id.clone())
        } else {
            req.with_members(vec![
                MemberIdentity::default().with_member_id(j.member_id.clone()),
            ])
        };
        let r = c.send(v, req).await;
        let code = if v < 3 {
            r.error_code
        } else {
            r.members[0].error_code
        };
        assert_eq!(code, 0, "LeaveGroup v{v}");
    }
    for v in range(ApiKey::OffsetCommit) {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(s("offsets")))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(common::topic_name("orders"))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(0),
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(9)
                            .with_committed_offset(0),
                    ]),
            ]);
        let r = c.send(v, req).await;
        let parts = &r.topics[0].partitions;
        assert_eq!(parts[0].error_code, 0, "OffsetCommit v{v}");
        assert_eq!(
            parts[1].error_code,
            ResponseError::UnknownTopicOrPartition.code(),
            "OffsetCommit v{v} error"
        );
    }
    for v in range(ApiKey::OffsetFetch) {
        if v < 8 {
            let req = OffsetFetchRequest::default()
                .with_group_id(GroupId(s("offsets")))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(common::topic_name("orders"))
                        .with_partition_indexes(vec![0, 9]),
                ]));
            let r = c.send(v, req).await;
            let parts = &r.topics[0].partitions;
            assert_eq!(
                (parts[0].error_code, parts[0].committed_offset),
                (0, 0),
                "OffsetFetch v{v}"
            );
            assert_eq!(
                parts[1].error_code,
                ResponseError::UnknownTopicOrPartition.code()
            );
        } else {
            let req = OffsetFetchRequest::default().with_groups(vec![
                OffsetFetchRequestGroup::default()
                    .with_group_id(GroupId(s("offsets")))
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .with_name(common::topic_name("orders"))
                            .with_partition_indexes(vec![0]),
                    ])),
            ]);
            let r = c.send(v, req).await;
            assert_eq!(
                r.groups[0].topics[0].partitions[0].committed_offset, 0,
                "OffsetFetch v{v}"
            );
        }
    }
    for v in range(ApiKey::ListGroups) {
        let r = c.send(v, ListGroupsRequest::default()).await;
        assert_eq!(r.error_code, 0, "ListGroups v{v}");
        assert!(!r.groups.is_empty());
    }
    for v in range(ApiKey::DescribeGroups) {
        let r = c
            .send(
                v,
                DescribeGroupsRequest::default()
                    .with_groups(vec![GroupId(s("hb")), GroupId(s("nope"))]),
            )
            .await;
        assert_eq!(r.groups[0].error_code, 0, "DescribeGroups v{v}");
        assert_eq!(&*r.groups[1].group_state, "Dead");
    }
    for v in range(ApiKey::InitProducerId) {
        let r = c
            .send(
                v,
                InitProducerIdRequest::default().with_transactional_id(None),
            )
            .await;
        assert_eq!(r.error_code, 0, "InitProducerId v{v}");
        let refused = c
            .send(
                v,
                InitProducerIdRequest::default().with_transactional_id(Some(
                    kafka_protocol::messages::TransactionalId(s("tx")),
                )),
            )
            .await;
        assert_eq!(
            refused.error_code,
            ResponseError::UnsupportedVersion.code(),
            "InitProducerId v{v} error"
        );
    }
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
