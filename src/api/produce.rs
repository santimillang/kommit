use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use crate::api::{log_error_code, records};
use crate::broker::Broker;
use crate::log::PartitionLog;
use crate::record::Offset;

/// How many recent batches per producer and partition are remembered for de-duplication
/// (Kafka keeps five: the most a producer may have in flight).
const REMEMBERED_BATCHES: usize = 5;

/// A producer's current epoch on one partition and its recent batches there:
/// (first sequence, last sequence, base offset).
#[derive(Default)]
struct RecentBatches {
    epoch: i16,
    batches: VecDeque<(i32, i32, Offset)>,
}

/// Per (producer id, topic, partition) memory of recent batches. Held across the
/// append, so a check and its append cannot interleave with another request's.
#[derive(Default)]
pub struct IdempotenceCache {
    batches: tokio::sync::Mutex<HashMap<(i64, String, i32), RecentBatches>>,
}

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
            let batches = match pd.records.map(records::decode_produce_batches).transpose() {
                Ok(batches) => batches.unwrap_or_default(),
                Err(e) => {
                    partitions.push(
                        base.with_error_code(ResponseError::CorruptMessage.code())
                            .with_error_message(Some(StrBytes::from_string(format!("{e:#}")))),
                    );
                    continue;
                }
            };
            let mut first_offset = None;
            let mut error = None;
            for batch in batches {
                match append_batch(broker, &name.0, pd.index, log, client_id, batch).await {
                    Ok(offset) => {
                        first_offset.get_or_insert(offset);
                    }
                    Err(code) => {
                        error = Some(code);
                        break;
                    }
                }
            }
            partitions.push(match error {
                Some(code) => base.with_error_code(code),
                None => base
                    .with_base_offset(first_offset.unwrap_or_else(|| log.high_watermark()))
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0),
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

/// Appends one batch. Idempotent batches (producer id >= 0) are de-duplicated: a
/// retry of a remembered batch returns its original offset without appending, and a
/// sequence gap is refused. Unknown producers (e.g. after a restart) start anywhere.
async fn append_batch(
    broker: &Broker,
    topic: &str,
    partition: i32,
    log: &Arc<dyn PartitionLog>,
    client_id: &str,
    batch: records::ProducedBatch,
) -> Result<Offset, i16> {
    if batch.producer_id < 0 {
        return log
            .append(client_id, batch.records)
            .await
            .map_err(|e| log_error_code(&e));
    }
    let mut cache = broker.idempotence.batches.lock().await;
    let recent = cache
        .entry((batch.producer_id, topic.to_string(), partition))
        .or_insert_with(|| RecentBatches {
            epoch: batch.producer_epoch,
            batches: VecDeque::new(),
        });
    // KIP-360: a producer bumps its epoch after a failure and restarts at sequence 0.
    // Older epochs are fenced; a newer one starts a fresh sequence space.
    if batch.producer_epoch < recent.epoch {
        return Err(ResponseError::InvalidProducerEpoch.code());
    }
    if batch.producer_epoch > recent.epoch {
        if batch.first_sequence != 0 {
            return Err(ResponseError::OutOfOrderSequenceNumber.code());
        }
        recent.epoch = batch.producer_epoch;
        recent.batches.clear();
    }
    if let Some(&(_, _, offset)) = recent
        .batches
        .iter()
        .find(|(first, _, _)| *first == batch.first_sequence)
    {
        return Ok(offset);
    }
    if let Some(&(_, last, _)) = recent.batches.back()
        && batch.first_sequence != last.wrapping_add(1)
    {
        return Err(ResponseError::OutOfOrderSequenceNumber.code());
    }
    let offset = log
        .append(client_id, batch.records)
        .await
        .map_err(|e| log_error_code(&e))?;
    recent
        .batches
        .push_back((batch.first_sequence, batch.last_sequence, offset));
    if recent.batches.len() > REMEMBERED_BATCHES {
        recent.batches.pop_front();
    }
    Ok(offset)
}
