//! Kafka RecordBatch bytes <-> kommit records.

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};

use crate::record::{Offset, Record};

/// Decodes (and decompresses) every batch, dropping control records.
pub fn decode_batches(bytes: Bytes) -> Result<Vec<(Offset, Record)>> {
    let mut buf = bytes;
    let sets = RecordBatchDecoder::decode_all(&mut buf)?;
    Ok(sets
        .into_iter()
        .flat_map(|set| set.records)
        .filter(|r| !r.control)
        .map(|r| {
            let headers = r
                .headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            (
                r.offset,
                Record {
                    timestamp_ms: r.timestamp,
                    key: r.key,
                    value: r.value,
                    headers,
                },
            )
        })
        .collect())
}

pub(crate) fn to_kafka(offset: Offset, r: &Record) -> KafkaRecord {
    KafkaRecord {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: 0,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: -1,
        timestamp: r.timestamp_ms,
        key: r.key.clone(),
        value: r.value.clone(),
        headers: r
            .headers
            .iter()
            .map(|(k, v)| (StrBytes::from_string(k.clone()), v.clone()))
            .collect::<IndexMap<_, _>>(),
    }
}

/// Encodes records as one uncompressed v2 batch; no records gives empty bytes.
pub fn encode_batch(records: &[(Offset, Record)]) -> Result<Bytes> {
    if records.is_empty() {
        return Ok(Bytes::new());
    }
    let kafka: Vec<KafkaRecord> = records.iter().map(|(o, r)| to_kafka(*o, r)).collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        kafka.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_roundtrip_with_offsets() {
        let recs = vec![(7, Record::text(1, "a")), (8, Record::text(2, "b"))];
        let bytes = encode_batch(&recs).unwrap();
        assert_eq!(decode_batches(bytes).unwrap(), recs);
        assert!(encode_batch(&[]).unwrap().is_empty());
    }

    #[test]
    fn every_kcat_compression_codec_decodes() {
        let rec = Record::text(1, "squeezed");
        for compression in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let mut buf = bytes::BytesMut::new();
            let k = to_kafka(0, &rec);
            RecordBatchEncoder::encode(
                &mut buf,
                [&k],
                &RecordEncodeOptions {
                    version: 2,
                    compression,
                },
            )
            .unwrap();
            assert_eq!(
                decode_batches(buf.freeze()).unwrap(),
                vec![(0, rec.clone())],
                "{compression:?}"
            );
        }
    }

    #[test]
    fn corrupted_batch_is_an_error() {
        let mut bytes = encode_batch(&[(0, Record::text(1, "a"))]).unwrap().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(decode_batches(Bytes::from(bytes)).is_err());
    }
}
