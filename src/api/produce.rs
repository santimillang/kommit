use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::{log_error_code, records};
use crate::broker::Broker;

/// Returns `None` for acks=0, which gets no response at all.
pub async fn handle(
    broker: &Broker,
    client_id: &str,
    req: ProduceRequest,
) -> Option<ProduceResponse> {
    let mut responses = Vec::new();
    for TopicProduceData {
        name,
        partition_data,
        ..
    } in req.topic_data
    {
        let topic = broker.topic(&name.0).await;
        let mut partitions = Vec::new();
        for pd in partition_data {
            let base = PartitionProduceResponse::default()
                .with_index(pd.index)
                .with_base_offset(-1);
            let Some(log) = topic.as_ref().and_then(|t| t.partition(pd.index)) else {
                partitions
                    .push(base.with_error_code(ResponseError::UnknownTopicOrPartition.code()));
                continue;
            };
            let decoded = match pd.records.map(records::decode_batches).transpose() {
                Ok(recs) => recs.unwrap_or_default(),
                Err(e) => {
                    partitions.push(
                        base.with_error_code(ResponseError::CorruptMessage.code())
                            .with_error_message(Some(StrBytes::from_string(format!("{e:#}")))),
                    );
                    continue;
                }
            };
            let records = decoded.into_iter().map(|(_, r)| r).collect();
            partitions.push(match log.append(client_id, records).await {
                Ok(offset) => base
                    .with_base_offset(offset)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0),
                Err(e) => base.with_error_code(log_error_code(&e)),
            });
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(name)
                .with_partition_responses(partitions),
        );
    }
    broker.notify_appended();
    if req.acks == 0 {
        return None;
    }
    Some(ProduceResponse::default().with_responses(responses))
}
