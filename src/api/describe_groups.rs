use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::{DescribeGroupsRequest, DescribeGroupsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::broker::Broker;

/// Unknown groups are reported as `Dead` without an error, as Kafka does.
pub fn handle(broker: &Broker, req: DescribeGroupsRequest) -> DescribeGroupsResponse {
    let groups = req
        .groups
        .into_iter()
        .map(|id| {
            let described = DescribedGroup::default().with_group_id(id.clone());
            let Some(view) = broker.coordinator.describe(&id.0) else {
                return described.with_group_state(StrBytes::from_static_str("Dead"));
            };
            described
                .with_group_state(StrBytes::from_static_str(view.state.as_str()))
                .with_protocol_type(StrBytes::from_string(view.protocol_type))
                .with_protocol_data(StrBytes::from_string(view.protocol_name))
                .with_members(
                    view.members
                        .into_iter()
                        .map(|m| {
                            DescribedGroupMember::default()
                                .with_member_id(StrBytes::from_string(m.member_id))
                                .with_client_id(StrBytes::from_string(m.client_id))
                                .with_client_host(StrBytes::from_string(m.client_host))
                                .with_member_metadata(m.metadata)
                                .with_member_assignment(m.assignment)
                        })
                        .collect(),
                )
        })
        .collect();
    DescribeGroupsResponse::default().with_groups(groups)
}
