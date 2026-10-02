use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{GroupId, ListGroupsRequest, ListGroupsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::RequestContext;
use crate::broker::Broker;

/// Every known group, including Empty ones that only have committed offsets.
pub fn handle(broker: &Broker, ctx: &RequestContext, req: ListGroupsRequest) -> ListGroupsResponse {
    let groups = broker
        .coordinator
        .list()
        .into_iter()
        .filter(|(_, state, _)| {
            req.states_filter.is_empty()
                || req
                    .states_filter
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(state.as_str()))
        })
        .map(|(id, state, protocol_type)| {
            let group = ListedGroup::default()
                .with_group_id(GroupId(StrBytes::from_string(id)))
                .with_protocol_type(StrBytes::from_string(protocol_type));
            match ctx.version {
                0..=3 => group,
                4 => group.with_group_state(StrBytes::from_static_str(state.as_str())),
                _ => group
                    .with_group_state(StrBytes::from_static_str(state.as_str()))
                    .with_group_type(StrBytes::from_static_str("classic")),
            }
        })
        .collect();
    ListGroupsResponse::default().with_groups(groups)
}
