use kafka_protocol::messages::{SyncGroupRequest, SyncGroupResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::RequestContext;
use crate::broker::Broker;

pub async fn handle(
    broker: &Broker,
    ctx: &RequestContext,
    req: SyncGroupRequest,
) -> SyncGroupResponse {
    let group = req.group_id.0.to_string();
    let assignments = req
        .assignments
        .into_iter()
        .map(|a| (a.member_id.to_string(), a.assignment))
        .collect();
    let outcome = broker
        .coordinator
        .sync(&group, req.generation_id, &req.member_id, assignments)
        .await;
    let response = match outcome {
        Ok(assignment) => SyncGroupResponse::default().with_assignment(assignment),
        Err(e) => SyncGroupResponse::default().with_error_code(e.code()),
    };
    // v5 echoes the group's protocol type and name so clients can check them.
    if ctx.version >= 5
        && let Some(view) = broker.coordinator.describe(&group)
    {
        return response
            .with_protocol_type(Some(StrBytes::from_string(view.protocol_type)))
            .with_protocol_name(Some(StrBytes::from_string(view.protocol_name)));
    }
    response
}
