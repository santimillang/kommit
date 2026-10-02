use kafka_protocol::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::broker::Broker;

/// Idempotent producers get a fresh id at epoch 0. Transactional ones are refused:
/// the Java client reports UNSUPPORTED_VERSION as "transactions not supported".
pub async fn handle(broker: &Broker, req: InitProducerIdRequest) -> InitProducerIdResponse {
    let refused = |code: i16| {
        InitProducerIdResponse::default()
            .with_error_code(code)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1)
    };
    // Idempotent producers send a null transactional id; treat an empty one the same.
    if req
        .transactional_id
        .as_ref()
        .is_some_and(|t| !t.0.is_empty())
    {
        return refused(ResponseError::UnsupportedVersion.code());
    }
    match broker.next_producer_id().await {
        Ok(id) => InitProducerIdResponse::default()
            .with_producer_id(ProducerId(id))
            .with_producer_epoch(0),
        Err(e) => {
            tracing::error!("allocating a producer id failed: {e:#}");
            refused(ResponseError::KafkaStorageError.code())
        }
    }
}
