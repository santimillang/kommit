use kafka_protocol::messages::{HeartbeatRequest, HeartbeatResponse};

use crate::broker::Broker;

pub fn handle(broker: &Broker, req: HeartbeatRequest) -> HeartbeatResponse {
    let code = broker
        .coordinator
        .heartbeat(&req.group_id.0, req.generation_id, &req.member_id)
        .err()
        .map_or(0, |e| e.code());
    HeartbeatResponse::default().with_error_code(code)
}
