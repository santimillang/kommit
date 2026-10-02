mod common;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    GroupId, HeartbeatRequest, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    SyncGroupRequest,
};
use kafka_protocol::protocol::StrBytes;

fn s(v: &str) -> StrBytes {
    StrBytes::from_string(v.to_string())
}

fn join(member: &str) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(GroupId(s("g")))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(s(member))
        .with_protocol_type(s("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(s("range"))
                .with_metadata(Bytes::from(format!("meta:{member}"))),
        ])
}

fn sync(
    generation: i32,
    member: &str,
    assignments: Vec<(String, &'static str)>,
) -> SyncGroupRequest {
    SyncGroupRequest::default()
        .with_group_id(GroupId(s("g")))
        .with_generation_id(generation)
        .with_member_id(s(member))
        .with_protocol_type(Some(s("consumer")))
        .with_protocol_name(Some(s("range")))
        .with_assignments(
            assignments
                .into_iter()
                .map(|(m, a)| {
                    SyncGroupRequestAssignment::default()
                        .with_member_id(s(&m))
                        .with_assignment(Bytes::from_static(a.as_bytes()))
                })
                .collect(),
        )
}

fn heartbeat(generation: i32, member: &str) -> HeartbeatRequest {
    HeartbeatRequest::default()
        .with_group_id(GroupId(s("g")))
        .with_generation_id(generation)
        .with_member_id(s(member))
}

#[tokio::test]
async fn two_clients_form_a_group_sync_and_rebalance_when_one_leaves() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut a = common::TestClient::connect(addr).await;
    let mut b = common::TestClient::connect(addr).await;

    let (ja, jb) = tokio::join!(a.send(9, join("")), b.send(9, join("")));
    assert_eq!((ja.error_code, jb.error_code), (0, 0));
    assert_eq!(ja.generation_id, jb.generation_id);
    assert_eq!(ja.leader, jb.leader);
    assert_eq!(ja.protocol_type.as_deref(), Some("consumer"));
    assert_eq!(ja.protocol_name.as_deref(), Some("range"));

    let leads_a = ja.leader == ja.member_id;
    let (mut leader, mut follower, jl, jf): (_, _, JoinGroupResponse, JoinGroupResponse) =
        if leads_a {
            (a, b, ja, jb)
        } else {
            (b, a, jb, ja)
        };
    assert_eq!(jl.members.len(), 2);
    assert!(jf.members.is_empty());
    let generation = jl.generation_id;
    let (lid, fid) = (jl.member_id.to_string(), jf.member_id.to_string());

    let assignments = vec![(lid.clone(), "for-leader"), (fid.clone(), "for-follower")];
    let (sf, sl) = tokio::join!(follower.send(5, sync(generation, &fid, vec![])), async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        leader.send(5, sync(generation, &lid, assignments)).await
    });
    assert_eq!((sf.error_code, sl.error_code), (0, 0));
    assert_eq!(sf.assignment, "for-follower");
    assert_eq!(sl.assignment, "for-leader");
    assert_eq!(sf.protocol_name.as_deref(), Some("range"));

    assert_eq!(
        leader.send(4, heartbeat(generation, &lid)).await.error_code,
        0
    );
    assert_eq!(
        follower
            .send(4, heartbeat(generation, &fid))
            .await
            .error_code,
        0
    );

    let left = leader
        .send(
            5,
            LeaveGroupRequest::default()
                .with_group_id(GroupId(s("g")))
                .with_members(vec![MemberIdentity::default().with_member_id(s(&lid))]),
        )
        .await;
    assert_eq!((left.error_code, left.members[0].error_code), (0, 0));
    assert_eq!(
        follower
            .send(4, heartbeat(generation, &fid))
            .await
            .error_code,
        ResponseError::RebalanceInProgress.code()
    );
    let rejoin = follower.send(9, join(&fid)).await;
    assert_eq!(rejoin.generation_id, generation + 1);
    assert_eq!(rejoin.leader, rejoin.member_id);
}

#[tokio::test]
async fn coordinator_errors_reach_the_wire() {
    let (addr, _broker) = common::start_mem_broker().await;
    let mut c = common::TestClient::connect(addr).await;
    let r = c.send(9, join("ghost")).await;
    assert_eq!(r.error_code, ResponseError::UnknownMemberId.code());
    let r = c.send(4, heartbeat(1, "ghost")).await;
    assert_eq!(r.error_code, ResponseError::UnknownMemberId.code());
    // v0-2 leave a single member
    let r = c
        .send(
            1,
            LeaveGroupRequest::default()
                .with_group_id(GroupId(s("g")))
                .with_member_id(s("ghost")),
        )
        .await;
    assert_eq!(r.error_code, ResponseError::UnknownMemberId.code());
}
