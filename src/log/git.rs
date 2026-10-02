use std::sync::{Arc, Mutex, RwLock};

use anyhow::bail;
use gix::ObjectId;

use crate::git::encode::{commit_to_record, record_to_commit, sentinel_commit, sentinel_of};
use crate::git::store::GitStore;
use crate::groups::group_ref_component;
use crate::log::{LogError, MAX_READ_RECORDS, PartitionLog, take_within_limit};
use crate::record::{Offset, Record, now_ms};

pub fn partition_ref(topic: &str, partition: i32) -> String {
    format!("refs/heads/{topic}/{partition}")
}

/// Where `group`'s committed offset for a partition lives. It points at first-parent
/// position `c` for committed offset `c`, so `git rev-list --count <it>..<branch>` is the lag.
pub fn group_ref(group: &str, topic: &str, partition: i32) -> String {
    format!(
        "refs/groups/{}/{topic}/{partition}",
        group_ref_component(group)
    )
}

pub struct GitLog {
    store: Arc<GitStore>,
    topic: String,
    partition: i32,
    ref_name: String,
    /// index[0] is the sentinel; record offset k is index[k + 1].
    index: RwLock<Vec<ObjectId>>,
    writer: tokio::sync::Mutex<()>,
    fault: Mutex<Option<String>>,
}

fn storage(e: impl std::fmt::Display) -> LogError {
    LogError::Storage(format!("{e:#}"))
}

/// How a partition branch that does not exist yet comes to be.
pub enum Origin {
    /// A new, empty partition rooted at its own sentinel.
    New,
    /// A fork created pointing at `at`, which must lie on a branch rooted at the sentinel
    /// of `<root>/<partition>`. `root` is the original topic, even for a fork of a fork.
    Fork { root: String, at: ObjectId },
}

/// Checks that `chain` (oldest first) starts at the sentinel of `expected`.
pub(crate) fn check_root(
    s: &GitStore,
    what: &str,
    chain: &[ObjectId],
    expected: &str,
) -> anyhow::Result<()> {
    match s.with_commit(chain[0], |c| Ok(sentinel_of(c)))? {
        Some(found) if found == expected => Ok(()),
        Some(found) => bail!("{what} starts at the sentinel of partition {found}, not {expected}"),
        None => bail!("{what} does not start at a kommit sentinel commit"),
    }
}

impl GitLog {
    pub async fn open_or_create(
        store: Arc<GitStore>,
        topic: &str,
        partition: i32,
    ) -> anyhow::Result<Self> {
        Self::open(store, topic, partition, Origin::New).await
    }

    pub async fn open(
        store: Arc<GitStore>,
        topic: &str,
        partition: i32,
        origin: Origin,
    ) -> anyhow::Result<Self> {
        let ref_name = partition_ref(topic, partition);
        let (s, r, t) = (store.clone(), ref_name.clone(), topic.to_string());
        let index = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<ObjectId>> {
            let expected = match &origin {
                Origin::New => format!("{t}/{partition}"),
                Origin::Fork { root, .. } => format!("{root}/{partition}"),
            };
            if let Some(head) = s.ref_target(&r)? {
                let chain = s.first_parent_chain(head)?;
                check_root(&s, &r, &chain, &expected)?;
                return Ok(chain);
            }
            match origin {
                Origin::New => {
                    let sentinel = sentinel_commit(&t, partition, s.empty_tree(), now_ms());
                    let id = s.write_commit(&sentinel)?;
                    s.cas_ref(&r, None, id, "kommit: create partition")?;
                    Ok(vec![id])
                }
                Origin::Fork { at, .. } => {
                    let chain = s.first_parent_chain(at)?;
                    check_root(&s, &format!("fork point {at} for {r}"), &chain, &expected)?;
                    s.cas_ref(&r, None, at, "kommit: branch partition")?;
                    Ok(chain)
                }
            }
        })
        .await??;
        Ok(GitLog {
            store,
            topic: topic.to_string(),
            partition,
            ref_name,
            index: RwLock::new(index),
            writer: tokio::sync::Mutex::new(()),
            fault: Mutex::new(None),
        })
    }
}

#[async_trait::async_trait]
impl PartitionLog for GitLog {
    async fn commit_offset(&self, group: &str, offset: Offset) -> Result<(), LogError> {
        let target = {
            let index = self.index.read().unwrap();
            usize::try_from(offset)
                .ok()
                .and_then(|c| index.get(c).copied())
                .ok_or(LogError::OutOfRange(offset))?
        };
        let (store, r) = (
            self.store.clone(),
            group_ref(group, &self.topic, self.partition),
        );
        tokio::task::spawn_blocking(move || {
            store.set_ref(&r, target, &format!("kommit: commit offset {offset}"))
        })
        .await
        .map_err(storage)?
        .map_err(storage)
    }

    async fn committed_offset(&self, group: &str) -> Result<Option<Offset>, LogError> {
        let (store, r) = (
            self.store.clone(),
            group_ref(group, &self.topic, self.partition),
        );
        let target = tokio::task::spawn_blocking(move || store.ref_target(&r))
            .await
            .map_err(storage)?
            .map_err(storage)?;
        let Some(target) = target else {
            return Ok(None);
        };
        let position = self
            .index
            .read()
            .unwrap()
            .iter()
            .rposition(|id| *id == target);
        if position.is_none() {
            tracing::warn!(group, partition = %self.ref_name, "committed offset ref points outside the partition; ignoring it");
        }
        Ok(position.map(|p| p as Offset))
    }

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
    async fn committed_offsets_are_refs_and_git_measures_the_lag() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("d.git");
        {
            let log = GitLog::open_or_create(store(&dir), "orders", 0)
                .await
                .unwrap();
            let recs = (0..5).map(|i| Record::text(i, "x")).collect();
            log.append("app", recs).await.unwrap();
            for group in ["billing", "my group", "a..b", "über", ""] {
                log.commit_offset(group, 2).await.unwrap();
            }
        }
        let lag = git(
            &repo,
            &[
                "rev-list",
                "--count",
                "refs/groups/billing/orders/0..orders/0",
            ],
        );
        assert_eq!(lag.trim(), "3");
        git(&repo, &["fsck", "--strict", "--no-dangling"]);

        // survives a reopen, for ref-hostile group ids too
        let log = GitLog::open_or_create(store(&dir), "orders", 0)
            .await
            .unwrap();
        for group in ["billing", "my group", "a..b", "über", ""] {
            assert_eq!(
                log.committed_offset(group).await.unwrap(),
                Some(2),
                "{group:?}"
            );
        }
        // offset 0 points at the sentinel: lag is the whole partition
        log.commit_offset("billing", 0).await.unwrap();
        let lag = git(
            &repo,
            &[
                "rev-list",
                "--count",
                "refs/groups/billing/orders/0..orders/0",
            ],
        );
        assert_eq!(lag.trim(), "5");
    }

    #[tokio::test]
    async fn another_partitions_branch_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        drop(
            GitLog::open_or_create(s.clone(), "payments", 0)
                .await
                .unwrap(),
        );
        let head = s.ref_target("refs/heads/payments/0").unwrap().unwrap();
        s.cas_ref("refs/heads/orders/0", None, head, "copied")
            .unwrap();
        let err = GitLog::open_or_create(s, "orders", 0).await.err().unwrap();
        assert!(err.to_string().contains("payments/0"), "{err:#}");
    }

    /// orders/0 with `n` records; returns its first-parent chain (sentinel first).
    async fn source(s: &Arc<GitStore>, n: i64) -> Vec<ObjectId> {
        let log = GitLog::open_or_create(s.clone(), "orders", 0)
            .await
            .unwrap();
        let recs = (0..n).map(|i| Record::text(i, &format!("r{i}"))).collect();
        log.append("app", recs).await.unwrap();
        let head = s.ref_target("refs/heads/orders/0").unwrap().unwrap();
        s.first_parent_chain(head).unwrap()
    }

    fn fork_at(at: ObjectId) -> Origin {
        Origin::Fork {
            root: "orders".into(),
            at,
        }
    }

    #[tokio::test]
    async fn a_fork_shares_commits_and_diverges_on_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let chain = source(&s, 3).await;
        // fork point 2: shares offsets 0 and 1
        let fork = GitLog::open(s.clone(), "replay", 0, fork_at(chain[2]))
            .await
            .unwrap();
        assert_eq!(fork.high_watermark(), 2);
        let orders = GitLog::open_or_create(s.clone(), "orders", 0)
            .await
            .unwrap();
        assert_eq!(
            fork.read(0, 1 << 20).await.unwrap(),
            orders.read(0, 1 << 20).await.unwrap()[..2].to_vec()
        );

        let repo = dir.path().join("d.git");
        let rev = |r: &str| git(&repo, &["rev-parse", r]).trim().to_string();
        assert_eq!(rev("replay/0"), rev("orders/0~1"));

        assert_eq!(
            fork.append("app", vec![Record::text(9, "mine")])
                .await
                .unwrap(),
            2
        );
        assert_eq!(orders.high_watermark(), 3);
        assert_eq!(rev("orders/0"), chain[3].to_string());
        assert_eq!(
            git(&repo, &["merge-base", "orders/0", "replay/0"]).trim(),
            chain[2].to_string()
        );
        git(&repo, &["fsck", "--strict", "--no-dangling"]);

        // reopening keeps the fork's own head, not the fork point
        drop(fork);
        let fork = GitLog::open(s.clone(), "replay", 0, fork_at(chain[2]))
            .await
            .unwrap();
        assert_eq!(fork.high_watermark(), 3);
    }

    #[tokio::test]
    async fn forking_at_zero_points_at_the_shared_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let chain = source(&s, 0).await;
        let fork = GitLog::open(s.clone(), "replay", 0, fork_at(chain[0]))
            .await
            .unwrap();
        assert_eq!(fork.high_watermark(), 0);
        assert!(fork.read(0, 1024).await.unwrap().is_empty());
        assert_eq!(
            fork.append("app", vec![Record::text(1, "a")])
                .await
                .unwrap(),
            0
        );
        let subjects = git(
            &dir.path().join("d.git"),
            &["log", "--format=%s", "replay/0"],
        );
        assert_eq!(subjects, "a\nkommit: partition orders/0 created\n");
    }

    #[tokio::test]
    async fn a_fork_of_a_fork_keeps_the_original_root() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let chain = source(&s, 2).await;
        let first = GitLog::open(s.clone(), "replay", 0, fork_at(chain[2]))
            .await
            .unwrap();
        first
            .append("app", vec![Record::text(5, "x")])
            .await
            .unwrap();
        let head = s.ref_target("refs/heads/replay/0").unwrap().unwrap();
        let second = GitLog::open(s.clone(), "replay2", 0, fork_at(head))
            .await
            .unwrap();
        assert_eq!(second.high_watermark(), 3);
    }

    #[tokio::test]
    async fn a_fork_point_outside_the_root_topic_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        drop(
            GitLog::open_or_create(s.clone(), "payments", 0)
                .await
                .unwrap(),
        );
        let foreign = s.ref_target("refs/heads/payments/0").unwrap().unwrap();
        let err = GitLog::open(s.clone(), "replay", 0, fork_at(foreign))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("payments/0"), "{err:#}");
        assert_eq!(s.ref_target("refs/heads/replay/0").unwrap(), None);

        // an existing fork opened as a plain topic is refused too
        let chain = source(&s, 1).await;
        drop(
            GitLog::open(s.clone(), "replay", 0, fork_at(chain[1]))
                .await
                .unwrap(),
        );
        let err = GitLog::open_or_create(s, "replay", 0).await.err().unwrap();
        assert!(err.to_string().contains("orders/0"), "{err:#}");
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
