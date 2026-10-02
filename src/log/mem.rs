use std::collections::HashMap;
use std::sync::RwLock;

use crate::log::{LogError, PartitionLog, take_within_limit};
use crate::record::{Offset, Record};

/// In-memory log used to test the protocol layer without Git.
#[derive(Default)]
pub struct MemLog {
    records: RwLock<Vec<Record>>,
    committed: RwLock<HashMap<String, Offset>>,
}

#[async_trait::async_trait]
impl PartitionLog for MemLog {
    async fn commit_offset(&self, group: &str, offset: Offset) -> Result<(), LogError> {
        if !(0..=self.high_watermark()).contains(&offset) {
            return Err(LogError::OutOfRange(offset));
        }
        self.committed
            .write()
            .unwrap()
            .insert(group.to_string(), offset);
        Ok(())
    }

    async fn committed_offset(&self, group: &str) -> Result<Option<Offset>, LogError> {
        Ok(self.committed.read().unwrap().get(group).copied())
    }

    async fn append(&self, _producer: &str, records: Vec<Record>) -> Result<Offset, LogError> {
        let mut log = self.records.write().unwrap();
        let base = log.len() as Offset;
        log.extend(records);
        Ok(base)
    }

    async fn read(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<(Offset, Record)>, LogError> {
        let log = self.records.read().unwrap();
        if from < 0 || from > log.len() as Offset {
            return Err(LogError::OutOfRange(from));
        }
        let tail = log[from as usize..]
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, r)| (from + i as Offset, r));
        Ok(take_within_limit(tail, max_bytes))
    }

    async fn offset_for_timestamp(&self, ts_ms: i64) -> Result<Option<(Offset, i64)>, LogError> {
        let log = self.records.read().unwrap();
        Ok(log
            .iter()
            .enumerate()
            .find(|(_, r)| r.timestamp_ms >= ts_ms)
            .map(|(i, r)| (i as Offset, r.timestamp_ms)))
    }

    fn high_watermark(&self) -> Offset {
        self.records.read().unwrap().len() as Offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mem_log_meets_the_contract() {
        crate::log::contract::run_all(&MemLog::default()).await;
    }
}
