//! `kommit branch`: a thin client over CreateTopics with kommit.branch configs.

use anyhow::{Result, bail};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{BrokerId, CreateTopicsRequest, ListOffsetsRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::branch::{BRANCH_AT, BRANCH_FROM};
use crate::net::client::Client;

const LATEST: i64 = -1;

fn config(name: &str, value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .with_name(StrBytes::from_string(name.to_string()))
        .with_value(Some(StrBytes::from_string(value.to_string())))
}

/// Forks `from` as `to` on the broker at `bootstrap`. Returns where each partition of the
/// fork starts: its high watermark, which is also how many records it shares with `from`.
pub async fn branch(bootstrap: &str, from: &str, to: &str, at: &str) -> Result<Vec<i64>> {
    let mut client = Client::connect(bootstrap).await?;
    let topic = TopicName(StrBytes::from_string(to.to_string()));
    let req = CreateTopicsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic.clone())
                .with_num_partitions(-1)
                .with_replication_factor(-1)
                .with_configs(vec![config(BRANCH_FROM, from), config(BRANCH_AT, at)]),
        ]);
    let resp = client.send(7, req).await?;
    let Some(result) = resp.topics.first() else {
        bail!("the broker answered without a result for {to}");
    };
    if result.error_code != 0 {
        let msg = result
            .error_message
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_default();
        bail!(
            "branching {from} as {to} failed (error {}): {msg}",
            result.error_code
        );
    }
    let req = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic)
                .with_partitions(
                    (0..result.num_partitions)
                        .map(|p| {
                            ListOffsetsPartition::default()
                                .with_partition_index(p)
                                .with_timestamp(LATEST)
                        })
                        .collect(),
                ),
        ]);
    let resp = client.send(6, req).await?;
    let mut partitions: Vec<(i32, i64)> = resp
        .topics
        .into_iter()
        .flat_map(|t| t.partitions)
        .map(|p| (p.partition_index, p.offset))
        .collect();
    partitions.sort();
    Ok(partitions.into_iter().map(|(_, offset)| offset).collect())
}
