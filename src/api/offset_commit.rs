use kafka_protocol::ResponseError;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::{OffsetCommitRequest, OffsetCommitResponse};

use crate::api::log_error_code;
use crate::broker::Broker;

/// Commits are checked against live group membership (generation -1 is a group-less
/// commit and always allowed), then stored per partition as refs.
pub async fn handle(broker: &Broker, req: OffsetCommitRequest) -> OffsetCommitResponse {
    let group = req.group_id.0.to_string();
    let allowed = broker.coordinator.validate_commit(
        &group,
        req.generation_id_or_member_epoch,
        &req.member_id,
    );
    if allowed.is_ok() {
        broker.coordinator.ensure_group(&group);
    }
    let mut topics = Vec::new();
    for t in req.topics {
        let topic = broker.topic(&t.name.0).await;
        let mut partitions = Vec::new();
        for p in t.partitions {
            let code = match (
                &allowed,
                topic
                    .as_ref()
                    .and_then(|tp| tp.partition(p.partition_index)),
            ) {
                (Err(e), _) => e.code(),
                (Ok(()), None) => ResponseError::UnknownTopicOrPartition.code(),
                (Ok(()), Some(log)) => match log.commit_offset(&group, p.committed_offset).await {
                    Ok(()) => 0,
                    Err(e) => log_error_code(&e),
                },
            };
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(p.partition_index)
                    .with_error_code(code),
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(t.name)
                .with_partitions(partitions),
        );
    }
    OffsetCommitResponse::default().with_topics(topics)
}
