use std::sync::{Arc, Mutex, RwLock};

use anyhow::bail;
use gix::ObjectId;

use crate::git::encode::{commit_to_record, is_sentinel, record_to_commit, sentinel_commit};
use crate::git::store::GitStore;
use crate::log::{LogError, MAX_READ_RECORDS, PartitionLog, take_within_limit};
use crate::record::{Offset, Record, now_ms};

pub fn partition_ref(topic: &str, partition: i32) -> String {
    format!("refs/heads/{topic}/{partition}")
}

pub struct GitLog {
    store: Arc<GitStore>,
    ref_name: String,
    /// index[0] is the sentinel; record offset k is index[k + 1].
    index: RwLock<Vec<ObjectId>>,
    writer: tokio::sync::Mutex<()>,
    fault: Mutex<Option<String>>,
}

fn storage(e: impl std::fmt::Display) -> LogError {
    LogError::Storage(format!("{e:#}"))
}

impl GitLog {
    pub async fn open_or_create(
        store: Arc<GitStore>,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<Self> {
        let ref_name = partition_ref(topic, partition);
        let (s, r, t) = (store.clone(), ref_name.clone(), topic.to_string());
        let index = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<ObjectId>> {
            match s.ref_target(&r)? {
                Some(head) => {
                    let chain = s.first_parent_chain(head)?;
                    if !s.with_commit(chain[0], |c| Ok(is_sentinel(c)))? {
                        bail!("{r} does not start at a kommit sentinel commit");
                    }
                    Ok(chain)
                }
                None => {
                    let sentinel = sentinel_commit(&t, partition, s.empty_tree(), now_ms());
                    let id = s.write_commit(&sentinel)?;
                    s.cas_ref(&r, None, id, "kommit: create partition")?;
                    Ok(vec![id])
                }
            }
        })
        .await??;
        Ok(GitLog {
            store,
            ref_name,
            index: RwLock::new(index),
            writer: tokio::sync::Mutex::new(()),
            fault: Mutex::new(None),
        })
    }
}

#[async_trait::async_trait]
impl PartitionLog for GitLog {
    async fn append(&self, producer: &str, records: Vec<Record>) -> Result<Offset, LogError> {
        let _writer = self.writer.lock().await;
        if let Some(fault) = self.fault.lock().unwrap().clone() {
            return Err(LogError::Faulted(fault));
        }
        let base = self.high_watermark();
        if records.is_empty() {
            return Ok(base);
        }
        let head = *self
            .index
            .read()
            .unwrap()
            .last()
            .expect("index always holds the sentinel");
        let (store, r, producer) = (
            self.store.clone(),
            self.ref_name.clone(),
            producer.to_string(),
        );
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<ObjectId>, LogError> {
            let mut ids = Vec::with_capacity(records.len());
            let mut parent = head;
            for rec in &records {
                let commit = record_to_commit(rec, &producer, parent, store.empty_tree());
                parent = store.write_commit(&commit).map_err(storage)?;
                ids.push(parent);
            }
            let current = store.ref_target(&r).map_err(storage)?;
            if current != Some(head) {
                return Err(LogError::Faulted(format!(
                    "{r} moved outside kommit (expected {head}, found {current:?})"
                )));
            }
            let msg = format!("kommit: produce {} record(s)", records.len());
            store
                .cas_ref(&r, Some(head), parent, &msg)
                .map_err(storage)?;
            Ok(ids)
        })
        .await
        .map_err(storage)?;
        match result {
            Ok(ids) => {
                self.index.write().unwrap().extend(ids);
                Ok(base)
            }
            Err(LogError::Faulted(msg)) => {
                tracing::error!(partition = %self.ref_name, "{msg}; partition is now faulted until restart");
                *self.fault.lock().unwrap() = Some(msg.clone());
                Err(LogError::Faulted(msg))
            }
            Err(e) => Err(e),
        }
    }

    async fn read(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<(Offset, Record)>, LogError> {
        let ids: Vec<ObjectId> = {
            let index = self.index.read().unwrap();
            let hw = index.len() as Offset - 1;
            if from < 0 || from > hw {
                return Err(LogError::OutOfRange(from));
            }
            index[(from as usize + 1)..]
                .iter()
                .take(MAX_READ_RECORDS)
                .copied()
                .collect()
        };
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(Offset, Record)>, LogError> {
            let mut decoded = Vec::with_capacity(ids.len());
            let mut bytes = 0usize;
            for (i, id) in ids.into_iter().enumerate() {
                let rec = store
                    .with_commit(id, |c| Ok(commit_to_record(c)?))
                    .map_err(storage)?;
                bytes += rec.approx_size();
                decoded.push((from + i as Offset, rec));
                if bytes > max_bytes {
                    break; // take_within_limit trims the overshoot; stop decoding early
                }
            }
            Ok(take_within_limit(decoded.into_iter(), max_bytes))
        })
        .await
        .map_err(storage)?
    }

    async fn offset_for_timestamp(&self, ts_ms: i64) -> Result<Option<(Offset, i64)>, LogError> {
        let ids: Vec<ObjectId> = self.index.read().unwrap()[1..].to_vec();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<(Offset, i64)>, LogError> {
            for (i, id) in ids.into_iter().enumerate() {
                let ts = store
                    .with_commit(id, |c| Ok(commit_to_record(c)?.timestamp_ms))
                    .map_err(storage)?;
                if ts >= ts_ms {
                    return Ok(Some((i as Offset, ts)));
                }
            }
            Ok(None)
        })
        .await
        .map_err(storage)?
    }

    fn high_watermark(&self) -> Offset {
        self.index.read().unwrap().len() as Offset - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::testutil::git;
    use crate::record::Record;

    fn store(dir: &tempfile::TempDir) -> Arc<GitStore> {
        Arc::new(GitStore::open_or_init(&dir.path().join("d.git")).unwrap())
    }

    #[tokio::test]
    async fn git_log_meets_the_contract() {
        let dir = tempfile::tempdir().unwrap();
        let log = GitLog::open_or_create(store(&dir), "orders", 0)
            .await
            .unwrap();
        crate::log::contract::run_all(&log).await;
    }

    #[tokio::test]
    async fn reopening_rebuilds_the_index_from_history() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = GitLog::open_or_create(store(&dir), "orders", 0)
                .await
                .unwrap();
            log.append("app", vec![Record::text(1, "a"), Record::text(2, "b")])
                .await
                .unwrap();
        }
        let log = GitLog::open_or_create(store(&dir), "orders", 0)
            .await
            .unwrap();
        assert_eq!(log.high_watermark(), 2);
        assert_eq!(
            log.read(1, 1024).await.unwrap(),
            vec![(1, Record::text(2, "b"))]
        );
    }

    #[tokio::test]
    async fn git_sees_the_topic_as_a_branch_of_commits() {
        let dir = tempfile::tempdir().unwrap();
        let log = GitLog::open_or_create(store(&dir), "orders", 0)
            .await
            .unwrap();
        log.append(
            "app",
            vec![Record::text(1, "first"), Record::text(2, "second")],
        )
        .await
        .unwrap();
        let repo = dir.path().join("d.git");
        let subjects = git(&repo, &["log", "--format=%s", "orders/0"]);
        assert_eq!(
            subjects,
            "second\nfirst\nkommit: partition orders/0 created\n"
        );
        assert_eq!(git(&repo, &["rev-list", "--count", "orders/0"]).trim(), "3");
        git(&repo, &["fsck", "--strict", "--no-dangling"]);
    }

    #[tokio::test]
    async fn a_branch_moved_behind_kommits_back_faults_the_partition() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let log = GitLog::open_or_create(s.clone(), "orders", 0)
            .await
            .unwrap();
        log.append("app", vec![Record::text(1, "a")]).await.unwrap();
        // someone else moves the branch back to the sentinel
        let head = s.ref_target("refs/heads/orders/0").unwrap().unwrap();
        let root = s.first_parent_chain(head).unwrap()[0];
        s.cas_ref("refs/heads/orders/0", Some(head), root, "vandal")
            .unwrap();

        let err = log
            .append("app", vec![Record::text(2, "b")])
            .await
            .unwrap_err();
        assert!(matches!(err, LogError::Faulted(_)), "{err:?}");
        // stays faulted even for a harmless append
        assert!(matches!(
            log.append("app", vec![Record::text(3, "c")]).await,
            Err(LogError::Faulted(_))
        ));
        // history was not rewritten by kommit
        assert_eq!(s.ref_target("refs/heads/orders/0").unwrap(), Some(root));
    }

    #[tokio::test]
    async fn a_branch_without_a_sentinel_root_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let mut plain = crate::git::encode::record_to_commit(
            &Record::text(1, "x"),
            "p",
            s.empty_tree(),
            s.empty_tree(),
        );
        // a root commit with no parents that is not a sentinel
        plain.parents.clear();
        let id = s.write_commit(&plain).unwrap();
        s.cas_ref("refs/heads/orders/0", None, id, "test").unwrap();
        let err = GitLog::open_or_create(s, "orders", 0).await.err().unwrap();
        assert!(err.to_string().contains("refs/heads/orders/0"), "{err:#}");
    }
}
