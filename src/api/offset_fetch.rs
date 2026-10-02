use kafka_protocol::ResponseError;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{OffsetFetchRequest, OffsetFetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::api::{RequestContext, log_error_code};
use crate::broker::Broker;

/// (partition, committed offset or -1, error code)
type Committed = (i32, i64, i16);

/// v1-7 ask about one group; v8+ batch several. Both shapes share `committed`.
pub async fn handle(
    broker: &Broker,
    ctx: &RequestContext,
    req: OffsetFetchRequest,
) -> OffsetFetchResponse {
    if ctx.version >= 8 {
        let mut groups = Vec::new();
        for g in req.groups {
            let wanted = g.topics.map(|ts| {
                ts.into_iter()
                    .map(|t| (t.name, t.partition_indexes))
                    .collect()
            });
            let topics = committed(broker, &g.group_id.0, wanted)
                .await
                .into_iter()
                .map(|(name, parts)| {
                    OffsetFetchResponseTopics::default()
                        .with_name(name)
                        .with_partitions(
                            parts
                                .into_iter()
                                .map(|(p, offset, code)| {
                                    OffsetFetchResponsePartitions::default()
                                        .with_partition_index(p)
                                        .with_committed_offset(offset)
                                        .with_metadata(Some(StrBytes::default()))
                                        .with_error_code(code)
                                })
                                .collect(),
                        )
                })
                .collect();
            groups.push(
                OffsetFetchResponseGroup::default()
                    .with_group_id(g.group_id)
                    .with_topics(topics),
            );
        }
        return OffsetFetchResponse::default().with_groups(groups);
    }
    let wanted = req.topics.map(|ts| {
        ts.into_iter()
            .map(|t| (t.name, t.partition_indexes))
            .collect()
    });
    let topics = committed(broker, &req.group_id.0, wanted)
        .await
        .into_iter()
        .map(|(name, parts)| {
            OffsetFetchResponseTopic::default()
                .with_name(name)
                .with_partitions(
                    parts
                        .into_iter()
                        .map(|(p, offset, code)| {
                            OffsetFetchResponsePartition::default()
                                .with_partition_index(p)
                                .with_committed_offset(offset)
                                .with_metadata(Some(StrBytes::default()))
                                .with_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect();
    OffsetFetchResponse::default().with_topics(topics)
}

/// Committed offsets for `group`: of the requested partitions (missing ones are -1),
/// or with `wanted = None`, of every partition that has one.
async fn committed(
    broker: &Broker,
    group: &str,
    wanted: Option<Vec<(TopicName, Vec<i32>)>>,
) -> Vec<(TopicName, Vec<Committed>)> {
    let mut out = Vec::new();
    match wanted {
        Some(topics) => {
            for (name, partitions) in topics {
                let topic = broker.topic(&name.0).await;
                let mut parts = Vec::new();
                for p in partitions {
                    parts.push(match topic.as_ref().and_then(|t| t.partition(p)) {
                        None => (p, -1, ResponseError::UnknownTopicOrPartition.code()),
                        Some(log) => match log.committed_offset(group).await {
                            Ok(offset) => (p, offset.unwrap_or(-1), 0),
                            Err(e) => (p, -1, log_error_code(&e)),
                        },
                    });
                }
                out.push((name, parts));
            }
        }
        None => {
            for (name, topic) in broker.topics().await {
                let mut parts = Vec::new();
                for (p, log) in topic.partitions.iter().enumerate() {
                    if let Ok(Some(offset)) = log.committed_offset(group).await {
                        parts.push((p as i32, offset, 0));
                    }
                }
                if !parts.is_empty() {
                    out.push((TopicName(StrBytes::from_string(name)), parts));
                }
            }
        }
    }
    out
}
