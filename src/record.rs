use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

/// A Kafka offset. Record offset `k` lives at first-parent position `k + 1` of its partition branch.
pub type Offset = i64;

/// A record as kommit stores it. Batch-level Kafka fields (producer id, sequence) are not kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

impl Record {
    /// A record with a text value and nothing else, used throughout the tests.
    pub fn text(timestamp_ms: i64, value: &str) -> Self {
        Record {
            timestamp_ms,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        }
    }

    /// Rough wire size, used to honour fetch byte limits.
    pub fn approx_size(&self) -> usize {
        let len = |b: &Option<Bytes>| b.as_ref().map_or(0, Bytes::len);
        let headers: usize = self.headers.iter().map(|(n, v)| n.len() + len(v)).sum();
        32 + len(&self.key) + len(&self.value) + headers
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
