use kafka_protocol::ResponseError;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::RequestContext;
use crate::broker::Broker;

const GROUP_KEY: i8 = 0;

/// kommit is the coordinator for every group. Transaction coordinators (key type 1)
/// do not exist, since transactions are not supported.
pub fn handle(
    broker: &Broker,
    ctx: &RequestContext,
    req: FindCoordinatorRequest,
) -> FindCoordinatorResponse {
    let cfg = &broker.config;
    let available = req.key_type == GROUP_KEY;
    let host = StrBytes::from_string(cfg.advertised_host.clone());
    if ctx.version < 4 {
        return if available {
            FindCoordinatorResponse::default()
                .with_node_id(BrokerId(cfg.node_id))
                .with_host(host)
                .with_port(cfg.advertised_port)
        } else {
            FindCoordinatorResponse::default()
                .with_error_code(ResponseError::CoordinatorNotAvailable.code())
                .with_node_id(BrokerId(-1))
                .with_port(-1)
        };
    }
    let coordinators = req
        .coordinator_keys
        .into_iter()
        .map(|key| {
            let entry = Coordinator::default().with_key(key);
            if available {
                entry
                    .with_node_id(BrokerId(cfg.node_id))
                    .with_host(host.clone())
                    .with_port(cfg.advertised_port)
            } else {
                entry
                    .with_error_code(ResponseError::CoordinatorNotAvailable.code())
                    .with_node_id(BrokerId(-1))
                    .with_port(-1)
            }
        })
        .collect();
    FindCoordinatorResponse::default().with_coordinators(coordinators)
}
