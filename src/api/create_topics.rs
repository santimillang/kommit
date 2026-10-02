use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::broker::Broker;

pub async fn handle(broker: &Broker, req: CreateTopicsRequest) -> CreateTopicsResponse {
    let mut results = Vec::new();
    for t in req.topics {
        let name = t.name.0.to_string();
        let partitions = if t.num_partitions == -1 {
            broker.config.default_partitions
        } else {
            t.num_partitions
        };
        let result = CreatableTopicResult::default()
            .with_name(t.name.clone())
            .with_num_partitions(partitions)
            .with_replication_factor(1);
        if t.replication_factor != -1 && t.replication_factor != 1 {
            results.push(
                result
                    .with_error_code(ResponseError::InvalidReplicationFactor.code())
                    .with_error_message(Some(StrBytes::from_static_str(
                        "kommit is a single broker",
                    ))),
            );
            continue;
        }
        let outcome = if req.validate_only {
            broker
                .validate_new_topic(&name, partitions)
                .await
                .map(|()| None)
        } else {
            broker
                .create_topic(&name, partitions)
                .await
                .map(|state| Some(state.topic_id))
        };
        results.push(match outcome {
            Ok(id) => result.with_topic_id(id.unwrap_or_default()),
            Err(e) => result
                .with_error_code(e.code())
                .with_error_message(Some(StrBytes::from_string(e.to_string()))),
        });
    }
    CreateTopicsResponse::default().with_topics(results)
}
