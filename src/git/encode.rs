//! Record <-> commit encoding. A Kafka record *is* a Git commit:
//! the value is the message, everything else lives in extra commit headers.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use gix::ObjectId;
use gix::bstr::{BString, ByteSlice};

use crate::record::Record;

pub const H_TS: &str = "kommit-ts";
pub const H_KEY: &str = "kommit-key";
pub const H_VALUE: &str = "kommit-value";
pub const H_HEADER: &str = "kommit-header";
pub const H_SENTINEL: &str = "kommit-sentinel";

/// Latest committer time Git renders sanely: 9999-12-31T23:59:59Z.
const MAX_GIT_SECONDS: i64 = 253_402_300_799;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("commit is a partition sentinel, not a record")]
    Sentinel,
    #[error("malformed kommit commit: {0}")]
    Malformed(String),
}

fn malformed(msg: impl Into<String>) -> DecodeError {
    DecodeError::Malformed(msg.into())
}

/// `t:<text>` for non-empty printable ASCII without spaces, otherwise `b:<base64>`.
fn field_enc(b: &[u8]) -> String {
    if !b.is_empty() && b.iter().all(|c| (0x21..=0x7e).contains(c)) {
        format!("t:{}", String::from_utf8_lossy(b))
    } else {
        format!("b:{}", B64.encode(b))
    }
}

fn field_dec(s: &str) -> Result<Bytes, DecodeError> {
    if let Some(text) = s.strip_prefix("t:") {
        Ok(Bytes::copy_from_slice(text.as_bytes()))
    } else if let Some(b64) = s.strip_prefix("b:") {
        B64.decode(b64)
            .map(Bytes::from)
            .map_err(|e| malformed(format!("bad base64 field: {e}")))
    } else {
        Err(malformed(format!("bad field {s:?}")))
    }
}

/// Git messages may hold any UTF-8 without NUL bytes; anything else is base64.
fn is_message_safe(v: &[u8]) -> bool {
    std::str::from_utf8(v).is_ok() && !v.contains(&0)
}

/// Git identities forbid `<`, `>` and newlines; keep the client id recognisable but safe.
pub fn sanitize_client_id(id: &str) -> String {
    let s: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() { "anonymous".into() } else { s }
}

pub fn kommit_signature(name: &str, ts_ms: i64) -> gix::actor::Signature {
    let seconds = ts_ms.div_euclid(1000).clamp(0, MAX_GIT_SECONDS);
    gix::actor::Signature {
        name: name.into(),
        email: format!("{name}@kommit").into(),
        time: gix::date::Time::new(seconds, 0),
    }
}

pub fn record_to_commit(
    rec: &Record,
    producer: &str,
    parent: ObjectId,
    tree: ObjectId,
) -> gix::objs::Commit {
    let mut extra: Vec<(BString, BString)> =
        vec![(H_TS.into(), rec.timestamp_ms.to_string().into())];
    if let Some(key) = &rec.key {
        extra.push((H_KEY.into(), field_enc(key).into()));
    }
    let message: BString = match &rec.value {
        None => {
            extra.push((H_VALUE.into(), "null".into()));
            BString::default()
        }
        Some(v) if is_message_safe(v) => v.to_vec().into(),
        Some(v) => {
            extra.push((H_VALUE.into(), "base64".into()));
            B64.encode(v).into()
        }
    };
    for (name, value) in &rec.headers {
        let value = value
            .as_deref()
            .map_or_else(|| "null".to_string(), field_enc);
        extra.push((
            H_HEADER.into(),
            format!("{} {value}", field_enc(name.as_bytes())).into(),
        ));
    }
    gix::objs::Commit {
        tree,
        parents: [parent].into_iter().collect(),
        author: kommit_signature(&sanitize_client_id(producer), rec.timestamp_ms),
        committer: kommit_signature("kommit", rec.timestamp_ms),
        encoding: None,
        message,
        extra_headers: extra,
    }
}

pub fn commit_to_record(c: &gix::objs::CommitRef<'_>) -> Result<Record, DecodeError> {
    let mut timestamp_ms = None;
    let mut key = None;
    let mut value_mode: Option<String> = None;
    let mut headers = Vec::new();
    for (name, value) in &c.extra_headers {
        let value = value
            .to_str()
            .map_err(|_| malformed("non-UTF-8 header value"))?;
        match name.to_str().unwrap_or_default() {
            H_SENTINEL => return Err(DecodeError::Sentinel),
            H_TS => {
                timestamp_ms = Some(
                    value
                        .parse::<i64>()
                        .map_err(|e| malformed(format!("bad {H_TS}: {e}")))?,
                )
            }
            H_KEY => key = Some(field_dec(value)?),
            H_VALUE => value_mode = Some(value.to_string()),
            H_HEADER => {
                let (n, v) = value
                    .split_once(' ')
                    .ok_or_else(|| malformed("bad record header"))?;
                let name = String::from_utf8(field_dec(n)?.to_vec())
                    .map_err(|_| malformed("header name is not UTF-8"))?;
                let v = if v == "null" {
                    None
                } else {
                    Some(field_dec(v)?)
                };
                headers.push((name, v));
            }
            _ => {}
        }
    }
    let message: &[u8] = c.message.as_ref();
    let value = match value_mode.as_deref() {
        None => Some(Bytes::copy_from_slice(message)),
        Some("null") => None,
        Some("base64") => Some(
            B64.decode(message)
                .map(Bytes::from)
                .map_err(|e| malformed(format!("bad base64 value: {e}")))?,
        ),
        Some(other) => return Err(malformed(format!("unknown {H_VALUE} {other:?}"))),
    };
    Ok(Record {
        timestamp_ms: timestamp_ms.ok_or_else(|| malformed(format!("missing {H_TS}")))?,
        key,
        value,
        headers,
    })
}

pub fn sentinel_commit(
    topic: &str,
    partition: i32,
    tree: ObjectId,
    now_ms: i64,
) -> gix::objs::Commit {
    let sig = kommit_signature("kommit", now_ms);
    gix::objs::Commit {
        tree,
        parents: Default::default(),
        author: sig.clone(),
        committer: sig,
        encoding: None,
        message: format!("kommit: partition {topic}/{partition} created\n").into(),
        extra_headers: vec![(H_SENTINEL.into(), format!("{topic}/{partition}").into())],
    }
}

/// The `<topic>/<partition>` a sentinel commit was created for, or `None` if `c` is not a sentinel.
pub fn sentinel_of(c: &gix::objs::CommitRef<'_>) -> Option<String> {
    c.extra_headers
        .iter()
        .find(|(name, _)| *name == H_SENTINEL)
        .map(|(_, value)| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use bytes::Bytes;
    use proptest::prelude::*;

    fn null_id() -> gix::ObjectId {
        gix::ObjectId::null(gix::hash::Kind::Sha1)
    }

    fn empty_tree_id() -> gix::ObjectId {
        gix::ObjectId::empty_tree(gix::hash::Kind::Sha1)
    }

    /// Writes the commit into a fresh bare repo and decodes it back, exactly as GitStore will.
    fn roundtrip(rec: &Record, producer: &str) -> Record {
        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init_bare(dir.path().join("r.git")).unwrap();
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();
        let parent = repo
            .write_object(sentinel_commit("t", 0, tree, 0))
            .unwrap()
            .detach();
        let id = repo
            .write_object(record_to_commit(rec, producer, parent, tree))
            .unwrap()
            .detach();
        let commit = repo.find_commit(id).unwrap();
        let decoded = commit.decode().unwrap();
        commit_to_record(&decoded).unwrap()
    }

    #[test]
    fn text_value_is_the_commit_message() {
        let rec = Record::text(1_700_000_000_123, "hello world");
        let commit = record_to_commit(&rec, "app", null_id(), empty_tree_id());
        assert_eq!(commit.message, "hello world");
        assert_eq!(commit.author.name, "app");
        assert_eq!(commit.committer.name, "kommit");
        assert_eq!(commit.committer.time.seconds, 1_700_000_000);
    }

    #[test]
    fn null_empty_and_binary_values_roundtrip() {
        for value in [
            None,
            Some(Bytes::new()),
            Some(Bytes::from_static(b"\x00\xffbin")),
        ] {
            let rec = Record {
                timestamp_ms: 5,
                key: None,
                value,
                headers: vec![],
            };
            assert_eq!(roundtrip(&rec, "p"), rec);
        }
    }

    #[test]
    fn keys_and_headers_roundtrip_including_empty_and_null() {
        let rec = Record {
            timestamp_ms: 5,
            key: Some(Bytes::new()),
            value: Some(Bytes::from_static(b"v")),
            headers: vec![
                ("trace id".into(), Some(Bytes::from_static(b"abc"))),
                ("".into(), None),
                ("bin".into(), Some(Bytes::from_static(b"\n\x00"))),
            ],
        };
        assert_eq!(roundtrip(&rec, "p"), rec);
    }

    #[test]
    fn negative_and_far_future_timestamps_roundtrip_with_clamped_committer_time() {
        for ts in [-1, i64::MIN, i64::MAX] {
            let rec = Record::text(ts, "x");
            let commit = record_to_commit(&rec, "p", null_id(), empty_tree_id());
            assert!((0..=253_402_300_799).contains(&commit.committer.time.seconds));
            assert_eq!(roundtrip(&rec, "p").timestamp_ms, ts);
        }
    }

    #[test]
    fn hostile_client_ids_are_sanitized() {
        assert_eq!(sanitize_client_id("rdkafka"), "rdkafka");
        assert_eq!(sanitize_client_id("a<b>\nc d"), "a_b__c_d");
        assert_eq!(sanitize_client_id(""), "anonymous");
    }

    #[test]
    fn sentinel_is_recognised_and_is_not_a_record() {
        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init_bare(dir.path().join("r.git")).unwrap();
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();
        let id = repo
            .write_object(sentinel_commit("orders", 3, tree, 0))
            .unwrap()
            .detach();
        let commit = repo.find_commit(id).unwrap();
        let decoded = commit.decode().unwrap();
        assert_eq!(sentinel_of(&decoded).as_deref(), Some("orders/3"));
        assert_eq!(commit_to_record(&decoded), Err(DecodeError::Sentinel));
        assert_eq!(decoded.message, "kommit: partition orders/3 created\n");
    }

    fn arb_bytes(max: usize) -> impl Strategy<Value = Option<Bytes>> {
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..max).prop_map(Bytes::from))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn any_record_roundtrips(
            ts in any::<i64>(),
            key in arb_bytes(48),
            value in arb_bytes(256),
            headers in proptest::collection::vec(("(?s).{0,12}", arb_bytes(24)), 0..4),
        ) {
            let rec = Record { timestamp_ms: ts, key, value, headers };
            prop_assert_eq!(roundtrip(&rec, "prop"), rec);
        }
    }
}
