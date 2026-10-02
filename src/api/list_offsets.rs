use kafka_protocol::ResponseError;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse};

use crate::api::log_error_code;
use crate::broker::Broker;

const LATEST: i64 = -1;
const EARLIEST: i64 = -2;

pub async fn handle(broker: &Broker, req: ListOffsetsRequest) -> ListOffsetsResponse {
    let mut topics = Vec::new();
    for t in req.topics {
        let topic = broker.topic(&t.name.0).await;
        let mut partitions = Vec::new();
        for p in t.partitions {
            let base = ListOffsetsPartitionResponse::default()
                .with_partition_index(p.partition_index)
                .with_offset(-1)
                .with_timestamp(-1)
                .with_leader_epoch(0);
            let Some(log) = topic
                .as_ref()
                .and_then(|tp| tp.partition(p.partition_index))
            else {
                partitions
                    .push(base.with_error_code(ResponseError::UnknownTopicOrPartition.code()));
                continue;
            };
            partitions.push(match p.timestamp {
                EARLIEST => base.with_offset(log.log_start()),
                LATEST => base.with_offset(log.high_watermark()),
                ts if ts >= 0 => match log.offset_for_timestamp(ts).await {
                    Ok(Some((offset, found_ts))) => {
                        base.with_offset(offset).with_timestamp(found_ts)
                    }
                    // Kafka answers "no record at or after ts" with offset -1 and no error.
                    Ok(None) => base,
                    Err(e) => base.with_error_code(log_error_code(&e)),
                },
                _ => base.with_error_code(ResponseError::InvalidRequest.code()),
            });
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(t.name)
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}
