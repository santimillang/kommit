#[cfg(test)]
pub mod contract;
pub mod git;
pub mod mem;

use crate::record::{Offset, Record};

/// Upper bound on records returned by one read, so a fetch never copies a whole topic.
pub const MAX_READ_RECORDS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LogError {
    #[error("offset {0} is out of range")]
    OutOfRange(Offset),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("partition is faulted: {0}")]
    Faulted(String),
}

#[async_trait::async_trait]
pub trait PartitionLog: Send + Sync {
    async fn append(&self, producer: &str, records: Vec<Record>) -> Result<Offset, LogError>;
    async fn read(&self, from: Offset, max_bytes: usize)
    -> Result<Vec<(Offset, Record)>, LogError>;
    async fn offset_for_timestamp(&self, ts_ms: i64) -> Result<Option<(Offset, i64)>, LogError>;
    fn high_watermark(&self) -> Offset;
    fn log_start(&self) -> Offset {
        0
    }
}

/// Applies the byte and count limits shared by every implementation's read path.
pub(crate) fn take_within_limit(
    records: impl Iterator<Item = (Offset, Record)>,
    max_bytes: usize,
) -> Vec<(Offset, Record)> {
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for (offset, record) in records.take(MAX_READ_RECORDS) {
        let size = record.approx_size();
        if !out.is_empty() && bytes + size > max_bytes {
            break;
        }
        bytes += size;
        out.push((offset, record));
    }
    out
}
