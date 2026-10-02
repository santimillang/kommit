use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::{LeaveGroupRequest, LeaveGroupResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::RequestContext;
use crate::broker::Broker;

/// v0-2 name one member; v3+ batch several, each with its own error.
pub fn handle(broker: &Broker, ctx: &RequestContext, req: LeaveGroupRequest) -> LeaveGroupResponse {
    let group = req.group_id.0.to_string();
    if ctx.version < 3 {
        let results = broker
            .coordinator
            .leave(&group, &[req.member_id.to_string()]);
        let code = results[0].1.err().map_or(0, |e| e.code());
        return LeaveGroupResponse::default().with_error_code(code);
    }
    let ids: Vec<String> = req
        .members
        .iter()
        .map(|m| m.member_id.to_string())
        .collect();
    let members = broker
        .coordinator
        .leave(&group, &ids)
        .into_iter()
        .map(|(id, result)| {
            MemberResponse::default()
                .with_member_id(StrBytes::from_string(id))
                .with_error_code(result.err().map_or(0, |e| e.code()))
        })
        .collect();
    LeaveGroupResponse::default().with_members(members)
}
