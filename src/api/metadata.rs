use kafka_protocol::ResponseError;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::broker::{Broker, TopicState};

pub async fn handle(broker: &Broker, req: MetadataRequest) -> MetadataResponse {
    let cfg = &broker.config;
    let node = BrokerId(cfg.node_id);
    let mut topics = Vec::new();
    match req.topics {
        None => {
            for (name, state) in broker.topics().await {
                topics.push(topic_entry(node, &name, &state));
            }
        }
        Some(requested) => {
            for t in requested {
                let Some(name) = t.name else { continue }; // lookups by topic id are not supported
                let name = name.0.to_string();
                let found = if req.allow_auto_topic_creation {
                    broker.get_or_auto_create(&name).await
                } else {
                    Ok(broker.topic(&name).await)
                };
                topics.push(match found {
                    Ok(Some(state)) => topic_entry(node, &name, &state),
                    Ok(None) => error_entry(&name, ResponseError::UnknownTopicOrPartition.code()),
                    // InvalidName maps to INVALID_TOPIC_EXCEPTION via CreateTopicError::code
                    Err(e) => error_entry(&name, e.code()),
                });
            }
        }
    }
    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(node)
                .with_host(StrBytes::from_string(cfg.advertised_host.clone()))
                .with_port(cfg.advertised_port),
        ])
        .with_cluster_id(Some(StrBytes::from_string(cfg.cluster_id.clone())))
        .with_controller_id(node)
        .with_topics(topics)
}

fn topic_name(name: &str) -> Option<TopicName> {
    Some(TopicName(StrBytes::from_string(name.to_string())))
}

fn topic_entry(node: BrokerId, name: &str, state: &TopicState) -> MetadataResponseTopic {
    let partitions = (0..state.partitions.len() as i32)
        .map(|p| {
            MetadataResponsePartition::default()
                .with_partition_index(p)
                .with_leader_id(node)
                .with_leader_epoch(0)
                .with_replica_nodes(vec![node])
                .with_isr_nodes(vec![node])
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(topic_name(name))
        .with_topic_id(state.topic_id)
        .with_partitions(partitions)
}

fn error_entry(name: &str, code: i16) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_name(topic_name(name))
        .with_error_code(code)
}
