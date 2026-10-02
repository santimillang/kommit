use std::sync::Arc;

use anyhow::{Context, Result, bail};
use gix::ObjectId;
use uuid::Uuid;

use crate::git::meta::{self, GroupOffset, MetaEvent};
use crate::git::store::GitStore;
use crate::groups::group_from_ref_component;
use crate::log::PartitionLog;
use crate::log::git::{GitLog, Origin, check_root, partition_ref};
use crate::log::mem::MemLog;
use crate::record::{Offset, now_ms};

/// A create or branch whose metadata commit landed but whose branches could not all be
/// opened. The topic is in the meta log, and the next startup creates what is missing.
#[derive(Debug, thiserror::Error)]
#[error("{0:#}; the topic is recorded, so restarting kommit will finish creating it")]
pub struct RecordedButNotOpened(pub anyhow::Error);

pub struct LoadedTopic {
    pub name: String,
    pub topic_id: Uuid,
    /// The topic whose sentinels this topic's branches start at: its own name unless
    /// it is a fork.
    pub root: String,
    pub partitions: Vec<Arc<dyn PartitionLog>>,
}

/// Everything a broker rebuilds from storage on startup.
#[derive(Default)]
pub struct Loaded {
    pub topics: Vec<LoadedTopic>,
    /// Groups that have committed offsets, sorted.
    pub groups: Vec<String>,
    /// Producer ids below this may already have been handed out.
    pub producer_id_high: i64,
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn load(&self) -> Result<Loaded>;
    async fn create_topic(
        &self,
        name: &str,
        topic_id: Uuid,
        partitions: i32,
    ) -> Result<Vec<Arc<dyn PartitionLog>>>;
    /// Creates `name` as a fork of `from`: partition p shares `from`'s first `at[p]`
    /// records. `root` is the topic `from`'s branches are rooted at. `source` holds
    /// `from`'s logs, for storages that copy instead of sharing commits. `groups` are
    /// committed offsets the fork starts with, each within its partition's fork point.
    #[allow(clippy::too_many_arguments)]
    async fn branch_topic(
        &self,
        name: &str,
        topic_id: Uuid,
        from: &str,
        root: &str,
        source: &[Arc<dyn PartitionLog>],
        at: &[Offset],
        groups: &[GroupOffset],
    ) -> Result<Vec<Arc<dyn PartitionLog>>>;
    /// Durably records that producer ids below `up_to` may be in use.
    async fn allocate_producer_ids(&self, up_to: i64) -> Result<()>;
}

pub struct MemStorage;

#[async_trait::async_trait]
impl Storage for MemStorage {
    async fn load(&self) -> Result<Loaded> {
        Ok(Loaded::default())
    }

    async fn create_topic(
        &self,
        _name: &str,
        _topic_id: Uuid,
        partitions: i32,
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        Ok((0..partitions)
            .map(|_| Arc::new(MemLog::default()) as Arc<dyn PartitionLog>)
            .collect())
    }

    async fn branch_topic(
        &self,
        _name: &str,
        _topic_id: Uuid,
        _from: &str,
        _root: &str,
        source: &[Arc<dyn PartitionLog>],
        at: &[Offset],
        groups: &[GroupOffset],
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let logs = Self::copy_prefixes(source, at).await?;
        apply_group_offsets(&logs, groups).await?;
        Ok(logs)
    }

    async fn allocate_producer_ids(&self, _up_to: i64) -> Result<()> {
        Ok(())
    }
}

impl MemStorage {
    async fn copy_prefixes(
        source: &[Arc<dyn PartitionLog>],
        at: &[Offset],
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let mut logs: Vec<Arc<dyn PartitionLog>> = Vec::with_capacity(source.len());
        for (log, &n) in source.iter().zip(at) {
            let want = usize::try_from(n)?;
            let mut records = Vec::with_capacity(want);
            while records.len() < want {
                let batch = log.read(records.len() as Offset, usize::MAX).await?;
                if batch.is_empty() {
                    bail!("source partition ended before offset {n}");
                }
                let remaining = want - records.len();
                records.extend(batch.into_iter().take(remaining).map(|(_, r)| r));
            }
            let fork = MemLog::default();
            fork.append("kommit", records).await?;
            logs.push(Arc::new(fork));
        }
        Ok(logs)
    }
}

/// Writes each group offset whose ref is missing. One that exists is left alone: the
/// group may have committed on the fork since the offsets were recorded.
async fn apply_group_offsets(logs: &[Arc<dyn PartitionLog>], groups: &[GroupOffset]) -> Result<()> {
    apply_group_offsets_as(logs, groups, Restore::Strict).await
}

#[derive(Clone, Copy, PartialEq)]
enum Restore {
    /// While branching: every offset lies within its fork point, so any failure is real.
    Strict,
    /// At startup: the fork may have been rewound by hand since, so an offset past its
    /// head is skipped with a warning instead of keeping the broker down.
    AtStartup,
}

async fn apply_group_offsets_as(
    logs: &[Arc<dyn PartitionLog>],
    groups: &[GroupOffset],
    mode: Restore,
) -> Result<()> {
    for g in groups {
        let log = usize::try_from(g.partition)
            .ok()
            .and_then(|p| logs.get(p))
            .with_context(|| {
                format!(
                    "group {} has an offset for missing partition {}",
                    g.group, g.partition
                )
            })?;
        if log.committed_offset(&g.group).await?.is_none() {
            match log.commit_offset(&g.group, g.offset).await {
                Err(crate::log::LogError::OutOfRange(_)) if mode == Restore::AtStartup => {
                    tracing::warn!(
                        group = %g.group,
                        partition = g.partition,
                        offset = g.offset,
                        high_watermark = log.high_watermark(),
                        "recorded group offset is past the fork's head; not restoring it"
                    );
                }
                other => other?,
            }
        }
    }
    Ok(())
}

pub struct GitStorage {
    store: Arc<GitStore>,
}

impl GitStorage {
    pub fn new(store: Arc<GitStore>) -> Self {
        GitStorage { store }
    }

    /// Opens every partition branch, creating any that are missing. Creating the
    /// missing ones repairs a crash between the metadata commit and branch creation.
    async fn open_partitions(
        &self,
        name: &str,
        partitions: i32,
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let mut logs: Vec<Arc<dyn PartitionLog>> = Vec::with_capacity(partitions as usize);
        for p in 0..partitions {
            logs.push(Arc::new(
                GitLog::open_or_create(self.store.clone(), name, p)
                    .await
                    .with_context(|| format!("opening partition {name}/{p}"))?,
            ));
        }
        Ok(logs)
    }

    /// Opens every partition branch of a fork, creating missing ones at their fork point.
    /// Creating them repairs a crash between the BranchTopic commit and the refs.
    async fn open_forks(
        &self,
        name: &str,
        root: &str,
        heads: &[ObjectId],
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let mut logs: Vec<Arc<dyn PartitionLog>> = Vec::with_capacity(heads.len());
        for (p, &at) in heads.iter().enumerate() {
            let origin = Origin::Fork {
                root: root.to_string(),
                at,
            };
            logs.push(Arc::new(
                GitLog::open(self.store.clone(), name, p as i32, origin)
                    .await
                    .with_context(|| format!("opening fork {name}/{p}"))?,
            ));
        }
        Ok(logs)
    }
}

#[async_trait::async_trait]
impl Storage for GitStorage {
    async fn load(&self) -> Result<Loaded> {
        let store = self.store.clone();
        let events = tokio::task::spawn_blocking(move || meta::replay(&store)).await??;
        let mut loaded = Loaded::default();
        for event in events {
            match event {
                MetaEvent::CreateTopic {
                    name,
                    partitions,
                    topic_id,
                } => {
                    let partitions = self.open_partitions(&name, partitions).await?;
                    loaded.topics.push(LoadedTopic {
                        root: name.clone(),
                        name,
                        topic_id,
                        partitions,
                    });
                }
                MetaEvent::BranchTopic {
                    name,
                    topic_id,
                    root,
                    heads,
                    group_offsets,
                    ..
                } => {
                    let heads = heads
                        .iter()
                        .map(|h| {
                            ObjectId::from_hex(h.as_bytes()).map_err(|e| {
                                anyhow::anyhow!("BranchTopic {name}: bad commit id {h}: {e}")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let partitions = self.open_forks(&name, &root, &heads).await?;
                    apply_group_offsets_as(&partitions, &group_offsets, Restore::AtStartup)
                        .await
                        .with_context(|| format!("restoring group offsets on fork {name}"))?;
                    loaded.topics.push(LoadedTopic {
                        name,
                        topic_id,
                        root,
                        partitions,
                    });
                }
                MetaEvent::AllocateProducerIds { up_to } => {
                    loaded.producer_id_high = loaded.producer_id_high.max(up_to);
                }
            }
        }
        // Listed after the replay, which may have repaired group refs.
        let store = self.store.clone();
        let group_refs =
            tokio::task::spawn_blocking(move || store.refs_with_prefix("refs/groups/")).await??;
        let mut groups: Vec<String> = group_refs
            .iter()
            .filter_map(|(name, _)| {
                let component = name.strip_prefix("refs/groups/")?.split('/').next()?;
                group_from_ref_component(component)
            })
            .collect();
        groups.sort();
        groups.dedup();
        loaded.groups = groups;
        Ok(loaded)
    }

    /// The metadata commit is the commit point: it is written before the branches.
    /// Branches that already exist (made by hand, or by another tool) are never adopted:
    /// creation fails before anything is recorded, so a restart is unaffected.
    async fn create_topic(
        &self,
        name: &str,
        topic_id: Uuid,
        partitions: i32,
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let store = self.store.clone();
        let event = MetaEvent::CreateTopic {
            name: name.to_string(),
            partitions,
            topic_id,
        };
        let topic = name.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            for p in 0..partitions {
                let r = partition_ref(&topic, p);
                if store.ref_target(&r)?.is_some() {
                    bail!("{r} already exists but is not a kommit topic; remove it first");
                }
            }
            meta::append(&store, &event, now_ms())
        })
        .await??;
        self.open_partitions(name, partitions)
            .await
            .map_err(|e| RecordedButNotOpened(e).into())
    }

    /// Like create_topic, the metadata commit is the commit point and existing refs are
    /// never adopted. Fork points come from the source's branches in Git: offset n is
    /// first-parent position n, the commit holding record n - 1.
    async fn branch_topic(
        &self,
        name: &str,
        topic_id: Uuid,
        from: &str,
        root: &str,
        _source: &[Arc<dyn PartitionLog>],
        at: &[Offset],
        groups: &[GroupOffset],
    ) -> Result<Vec<Arc<dyn PartitionLog>>> {
        let store = self.store.clone();
        let (topic, source, root_name, offsets, group_offsets) = (
            name.to_string(),
            from.to_string(),
            root.to_string(),
            at.to_vec(),
            groups.to_vec(),
        );
        let heads = tokio::task::spawn_blocking(move || -> Result<Vec<ObjectId>> {
            let mut heads = Vec::with_capacity(offsets.len());
            for (p, &n) in offsets.iter().enumerate() {
                let p = p as i32;
                let target = partition_ref(&topic, p);
                if store.ref_target(&target)?.is_some() {
                    bail!("{target} already exists but is not a kommit topic; remove it first");
                }
                let src = partition_ref(&source, p);
                let head = store
                    .ref_target(&src)?
                    .with_context(|| format!("{src} is missing"))?;
                let chain = store.first_parent_chain(head)?;
                // Checked before the meta commit: a fork recorded with a bad root could
                // never be opened, and the broker would refuse to start.
                check_root(&store, &src, &chain, &format!("{root_name}/{p}"))?;
                let id = usize::try_from(n)
                    .ok()
                    .and_then(|i| chain.get(i).copied())
                    .with_context(|| format!("{src} has no offset {n}"))?;
                heads.push(id);
            }
            let event = MetaEvent::BranchTopic {
                name: topic,
                topic_id,
                from: source,
                root: root_name,
                at: offsets,
                heads: heads.iter().map(|h| h.to_string()).collect(),
                group_offsets,
            };
            meta::append(&store, &event, now_ms())?;
            Ok(heads)
        })
        .await??;
        let opened = async {
            let logs = self.open_forks(name, root, &heads).await?;
            apply_group_offsets(&logs, groups).await?;
            Ok(logs)
        };
        opened
            .await
            .map_err(|e: anyhow::Error| RecordedButNotOpened(e).into())
    }

    async fn allocate_producer_ids(&self, up_to: i64) -> Result<()> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            meta::append(&store, &MetaEvent::AllocateProducerIds { up_to }, now_ms())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::testutil::git;
    use crate::record::Record;
    use std::path::Path;

    fn open(path: &Path) -> GitStorage {
        GitStorage::new(Arc::new(GitStore::open_or_init(path).unwrap()))
    }

    async fn orders(storage: &GitStorage) -> Vec<Arc<dyn PartitionLog>> {
        let logs = storage
            .create_topic("orders", Uuid::new_v4(), 2)
            .await
            .unwrap();
        let recs = |n: i64| (0..n).map(|i| Record::text(i, &format!("r{i}"))).collect();
        logs[0].append("app", recs(3)).await.unwrap();
        logs[1].append("app", recs(1)).await.unwrap();
        logs
    }

    fn go(group: &str, partition: i32, offset: Offset) -> GroupOffset {
        GroupOffset {
            group: group.into(),
            partition,
            offset,
        }
    }

    #[tokio::test]
    async fn a_branch_takes_group_offsets_along_as_shared_refs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let storage = open(&path);
            let source = orders(&storage).await;
            source[0].commit_offset("billing", 2).await.unwrap();
            source[1].commit_offset("my group", 1).await.unwrap();
            let groups = [go("billing", 0, 2), go("my group", 1, 1)];
            let fork = storage
                .branch_topic(
                    "replay",
                    Uuid::new_v4(),
                    "orders",
                    "orders",
                    &source,
                    &[3, 1],
                    &groups,
                )
                .await
                .unwrap();
            assert_eq!(fork[0].committed_offset("billing").await.unwrap(), Some(2));
            assert_eq!(fork[1].committed_offset("my group").await.unwrap(), Some(1));
            assert_eq!(fork[1].committed_offset("billing").await.unwrap(), None);
        }
        let rev = |r: &str| git(&path, &["rev-parse", r]).trim().to_string();
        assert_eq!(
            rev("refs/groups/billing/replay/0"),
            rev("refs/groups/billing/orders/0")
        );
        let lag = git(
            &path,
            &[
                "rev-list",
                "--count",
                "refs/groups/billing/replay/0..replay/0",
            ],
        );
        assert_eq!(lag.trim(), "1");
        let meta = git(&path, &["log", "-1", "--format=%B", meta::META_REF]);
        assert!(meta.contains("group = \"billing\""), "{meta}");
        git(&path, &["fsck", "--strict", "--no-dangling"]);

        let loaded = open(&path).load().await.unwrap();
        let replay = loaded.topics.iter().find(|t| t.name == "replay").unwrap();
        assert_eq!(
            replay.partitions[0]
                .committed_offset("billing")
                .await
                .unwrap(),
            Some(2)
        );
        assert!(loaded.groups.contains(&"my group".to_string()));
    }

    #[tokio::test]
    async fn recorded_group_offsets_are_repaired_but_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let storage = open(&path);
            orders(&storage).await;
            drop(storage);
            let store = GitStore::open_or_init(&path).unwrap();
            let head = store.ref_target("refs/heads/orders/0").unwrap().unwrap();
            // the meta commit landed; neither the fork's refs nor its group refs did
            meta::append(
                &store,
                &MetaEvent::BranchTopic {
                    name: "replay".into(),
                    topic_id: Uuid::new_v4(),
                    from: "orders".into(),
                    root: "orders".into(),
                    at: vec![3, 1],
                    heads: vec![
                        head.to_string(),
                        store
                            .ref_target("refs/heads/orders/1")
                            .unwrap()
                            .unwrap()
                            .to_string(),
                    ],
                    group_offsets: vec![go("billing", 0, 2)],
                },
                now_ms(),
            )
            .unwrap();
        }
        {
            let loaded = open(&path).load().await.unwrap();
            let replay = loaded.topics.iter().find(|t| t.name == "replay").unwrap();
            assert_eq!(
                replay.partitions[0]
                    .committed_offset("billing")
                    .await
                    .unwrap(),
                Some(2)
            );
            // the group moves on, on the fork
            replay.partitions[0]
                .commit_offset("billing", 0)
                .await
                .unwrap();
        }
        let loaded = open(&path).load().await.unwrap();
        let replay = loaded.topics.iter().find(|t| t.name == "replay").unwrap();
        assert_eq!(
            replay.partitions[0]
                .committed_offset("billing")
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn a_recorded_group_offset_past_a_rewound_fork_does_not_stop_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let storage = open(&path);
            let source = orders(&storage).await;
            source[0].commit_offset("billing", 3).await.unwrap();
            storage
                .branch_topic(
                    "replay",
                    Uuid::new_v4(),
                    "orders",
                    "orders",
                    &source,
                    &[3, 1],
                    &[go("billing", 0, 3)],
                )
                .await
                .unwrap();
            // an operator rewinds the fork by hand and deletes the group's ref
            let s = &storage.store;
            let head = s.ref_target("refs/heads/replay/0").unwrap().unwrap();
            let one = s.first_parent_chain(head).unwrap()[1];
            s.cas_ref("refs/heads/replay/0", Some(head), one, "rewind")
                .unwrap();
            git(&path, &["update-ref", "-d", "refs/groups/billing/replay/0"]);
        }
        let loaded = open(&path).load().await.unwrap();
        let replay = loaded.topics.iter().find(|t| t.name == "replay").unwrap();
        assert_eq!(replay.partitions[0].high_watermark(), 1);
        assert_eq!(
            replay.partitions[0]
                .committed_offset("billing")
                .await
                .unwrap(),
            None
        );
    }

    #[test]
    fn branch_events_from_before_group_offsets_still_load() {
        let old = "event = \"BranchTopic\"\nname = \"replay\"\n\
                   topic_id = \"6f1c3b5e-8a51-4a7e-9a4b-2f0d7c9e1a11\"\n\
                   from = \"orders\"\nroot = \"orders\"\nat = [1]\nheads = [\"ab\"]\n";
        match toml::from_str::<MetaEvent>(old).unwrap() {
            MetaEvent::BranchTopic { group_offsets, .. } => assert!(group_offsets.is_empty()),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_git_branch_shares_commits_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let id = Uuid::new_v4();
        {
            let storage = open(&path);
            let source = orders(&storage).await;
            // the source grew past the resolved point: the fork still starts at it
            source[0]
                .append("app", vec![Record::text(9, "late")])
                .await
                .unwrap();
            let fork = storage
                .branch_topic("replay", id, "orders", "orders", &source, &[2, 1], &[])
                .await
                .unwrap();
            assert_eq!(fork[0].high_watermark(), 2);
            assert_eq!(fork[1].high_watermark(), 1);
        }
        let rev = |r: &str| git(&path, &["rev-parse", r]).trim().to_string();
        assert_eq!(rev("replay/0"), rev("orders/0~2"));
        assert_eq!(rev("replay/1"), rev("orders/1"));
        let meta = git(&path, &["log", "-1", "--format=%B", meta::META_REF]);
        assert!(meta.contains("event = \"BranchTopic\""), "{meta}");
        assert!(meta.contains("at = [2, 1]"), "{meta}");
        git(&path, &["fsck", "--strict", "--no-dangling"]);

        let loaded = open(&path).load().await.unwrap();
        let names: Vec<(&str, &str)> = loaded
            .topics
            .iter()
            .map(|t| (t.name.as_str(), t.root.as_str()))
            .collect();
        assert_eq!(names, vec![("orders", "orders"), ("replay", "orders")]);
        let replay = &loaded.topics[1];
        assert_eq!(replay.topic_id, id);
        assert_eq!(replay.partitions[0].high_watermark(), 2);
    }

    #[tokio::test]
    async fn a_crash_before_the_fork_refs_is_repaired_at_the_recorded_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let fork_point = {
            let storage = open(&path);
            orders(&storage).await;
            drop(storage); // release the repository lock
            let store = GitStore::open_or_init(&path).unwrap();
            let head = store.ref_target("refs/heads/orders/0").unwrap().unwrap();
            let two = store.first_parent_chain(head).unwrap()[2];
            let p1 = store.ref_target("refs/heads/orders/1").unwrap().unwrap();
            // the meta commit landed; the refs did not
            meta::append(
                &store,
                &MetaEvent::BranchTopic {
                    name: "replay".into(),
                    topic_id: Uuid::new_v4(),
                    from: "orders".into(),
                    root: "orders".into(),
                    at: vec![2, 1],
                    heads: vec![two.to_string(), p1.to_string()],
                    group_offsets: vec![],
                },
                now_ms(),
            )
            .unwrap();
            two
        };
        let loaded = open(&path).load().await.unwrap();
        let replay = loaded.topics.iter().find(|t| t.name == "replay").unwrap();
        assert_eq!(replay.partitions[0].high_watermark(), 2);
        assert_eq!(
            git(&path, &["rev-parse", "replay/0"]).trim(),
            fork_point.to_string()
        );
    }

    #[tokio::test]
    async fn branching_over_an_existing_ref_records_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let storage = open(&path);
            let source = orders(&storage).await;
            let s = &storage.store;
            let head = s.ref_target("refs/heads/orders/1").unwrap().unwrap();
            s.cas_ref("refs/heads/replay/1", None, head, "hand-made")
                .unwrap();
            let err = storage
                .branch_topic(
                    "replay",
                    Uuid::new_v4(),
                    "orders",
                    "orders",
                    &source,
                    &[0, 0],
                    &[],
                )
                .await
                .err()
                .unwrap();
            assert!(err.to_string().contains("refs/heads/replay/1"), "{err:#}");
            assert_eq!(s.ref_target("refs/heads/replay/0").unwrap(), None);
        }
        let loaded = open(&path).load().await.unwrap();
        assert_eq!(loaded.topics.len(), 1);
    }

    #[tokio::test]
    async fn branching_a_source_moved_onto_another_topic_records_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let storage = open(&path);
            let source = orders(&storage).await;
            storage
                .create_topic("payments", Uuid::new_v4(), 1)
                .await
                .unwrap();
            // someone points orders/1 at payments' history behind kommit's back
            let s = &storage.store;
            let old = s.ref_target("refs/heads/orders/1").unwrap().unwrap();
            let foreign = s.ref_target("refs/heads/payments/0").unwrap().unwrap();
            s.cas_ref("refs/heads/orders/1", Some(old), foreign, "vandal")
                .unwrap();
            let err = storage
                .branch_topic(
                    "replay",
                    Uuid::new_v4(),
                    "orders",
                    "orders",
                    &source,
                    &[1, 0],
                    &[],
                )
                .await
                .err()
                .unwrap();
            assert!(err.to_string().contains("payments/0"), "{err:#}");
            assert_eq!(s.ref_target("refs/heads/replay/0").unwrap(), None);
            // put orders/1 back so the restart below only tests the branch
            s.cas_ref("refs/heads/orders/1", Some(foreign), old, "repair")
                .unwrap();
        }
        let loaded = open(&path).load().await.unwrap();
        assert!(loaded.topics.iter().all(|t| t.name != "replay"));
    }

    #[tokio::test]
    async fn a_fork_that_cannot_be_opened_is_named_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let store = GitStore::open_or_init(&path).unwrap();
            let missing = "ab".repeat(20);
            meta::append(
                &store,
                &MetaEvent::BranchTopic {
                    name: "replay".into(),
                    topic_id: Uuid::new_v4(),
                    from: "orders".into(),
                    root: "orders".into(),
                    at: vec![0],
                    heads: vec![missing],
                    group_offsets: vec![],
                },
                now_ms(),
            )
            .unwrap();
        }
        let err = open(&path).load().await.err().unwrap();
        assert!(format!("{err:#}").contains("replay/0"), "{err:#}");
    }

    #[tokio::test]
    async fn a_mem_branch_copies_the_shared_prefix() {
        let source = MemStorage
            .create_topic("orders", Uuid::new_v4(), 1)
            .await
            .unwrap();
        let recs = (0..3).map(|i| Record::text(i, "x")).collect();
        source[0].append("app", recs).await.unwrap();
        let fork = MemStorage
            .branch_topic(
                "replay",
                Uuid::new_v4(),
                "orders",
                "orders",
                &source,
                &[2],
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            fork[0].read(0, 1 << 20).await.unwrap(),
            source[0].read(0, 1 << 20).await.unwrap()[..2].to_vec()
        );
        fork[0]
            .append("app", vec![Record::text(7, "mine")])
            .await
            .unwrap();
        assert_eq!(source[0].high_watermark(), 3);
    }
}
