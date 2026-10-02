use std::time::Duration;

use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::{JoinGroupRequest, JoinGroupResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::RequestContext;
use crate::broker::Broker;
use crate::groups::coordinator::JoinRequest;

fn millis(ms: i32) -> Duration {
    Duration::from_millis(ms.max(0) as u64)
}

pub async fn handle(
    broker: &Broker,
    ctx: &RequestContext,
    req: JoinGroupRequest,
) -> JoinGroupResponse {
    // v0 has no rebalance timeout; Kafka uses the session timeout then.
    let rebalance_timeout = if ctx.version >= 1 {
        req.rebalance_timeout_ms
    } else {
        req.session_timeout_ms
    };
    let outcome = broker
        .coordinator
        .join(JoinRequest {
            group: req.group_id.0.to_string(),
            member_id: req.member_id.to_string(),
            client_id: ctx.client_id.clone(),
            client_host: ctx.client_host.clone(),
            session_timeout: millis(req.session_timeout_ms),
            rebalance_timeout: millis(rebalance_timeout),
            protocol_type: req.protocol_type.to_string(),
            protocols: req
                .protocols
                .into_iter()
                .map(|p| (p.name.to_string(), p.metadata))
                .collect(),
        })
        .await;
    match outcome {
        Ok(joined) => {
            let response = JoinGroupResponse::default()
                .with_generation_id(joined.generation)
                .with_protocol_name(Some(StrBytes::from_string(joined.protocol_name)))
                .with_leader(StrBytes::from_string(joined.leader))
                .with_member_id(StrBytes::from_string(joined.member_id))
                .with_members(
                    joined
                        .members
                        .into_iter()
                        .map(|(id, metadata)| {
                            JoinGroupResponseMember::default()
                                .with_member_id(StrBytes::from_string(id))
                                .with_metadata(metadata)
                        })
                        .collect(),
                );
            if ctx.version >= 7 {
                response.with_protocol_type(Some(StrBytes::from_string(joined.protocol_type)))
            } else {
                response
            }
        }
        // protocol_name only became nullable in v7; older versions need a string.
        Err(e) => JoinGroupResponse::default()
            .with_error_code(e.code())
            .with_generation_id(-1)
            .with_protocol_name(Some(StrBytes::default()))
            .with_member_id(req.member_id),
    }
}
