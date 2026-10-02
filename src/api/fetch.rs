use std::time::Duration;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse};
use tokio::time::Instant;

use crate::api::{log_error_code, records};
use crate::broker::Broker;

/// Serves a fetch, long-polling up to `max_wait_ms` for data when there is none yet.
pub async fn handle(broker: &Broker, req: FetchRequest) -> FetchResponse {
    let deadline = Instant::now() + Duration::from_millis(req.max_wait_ms.max(0) as u64);
    let mut appended = broker.subscribe();
    loop {
        appended.borrow_and_update();
        let (responses, bytes, errors) = fetch_once(broker, &req).await;
        if bytes > 0 || errors || req.max_wait_ms <= 0 {
            return response(responses);
        }
        match tokio::time::timeout_at(deadline, appended.changed()).await {
            Ok(Ok(())) => continue,
            _ => return response(responses),
        }
    }
}

fn response(responses: Vec<FetchableTopicResponse>) -> FetchResponse {
    FetchResponse::default()
        .with_session_id(0)
        .with_responses(responses)
}

async fn fetch_once(
    broker: &Broker,
    req: &FetchRequest,
) -> (Vec<FetchableTopicResponse>, usize, bool) {
    let mut total = 0usize;
    let mut any_error = false;
    let mut topics = Vec::new();
    for t in &req.topics {
        let topic = broker.topic(&t.topic.0).await;
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let base = PartitionData::default()
                .with_partition_index(p.partition)
                .with_preferred_read_replica(BrokerId(-1))
                .with_log_start_offset(0);
            let Some(log) = topic.as_ref().and_then(|tp| tp.partition(p.partition)) else {
                any_error = true;
                partitions.push(
                    base.with_error_code(ResponseError::UnknownTopicOrPartition.code())
                        .with_high_watermark(-1)
                        .with_last_stable_offset(-1),
                );
                continue;
            };
            let hw = log.high_watermark();
            let base = base.with_high_watermark(hw).with_last_stable_offset(hw);
            if total >= req.max_bytes.max(0) as usize {
                partitions.push(base.with_records(Some(Default::default())));
                continue;
            }
            let read = log
                .read(p.fetch_offset, p.partition_max_bytes.max(0) as usize)
                .await;
            partitions.push(match read.map(|recs| records::encode_batch(&recs)) {
                Ok(Ok(bytes)) => {
                    total += bytes.len();
                    base.with_records(Some(bytes))
                }
                Ok(Err(e)) => {
                    tracing::error!("encoding fetch response failed: {e:#}");
                    any_error = true;
                    base.with_error_code(ResponseError::UnknownServerError.code())
                }
                Err(e) => {
                    any_error = true;
                    base.with_error_code(log_error_code(&e))
                }
            });
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(t.topic.clone())
                .with_partitions(partitions),
        );
    }
    (topics, total, any_error)
}
