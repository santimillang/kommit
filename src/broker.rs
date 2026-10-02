use std::collections::BTreeMap;
use std::sync::Arc;

use kafka_protocol::ResponseError;
use tokio::sync::{RwLock, watch};
use uuid::Uuid;

use crate::api::produce::IdempotenceCache;
use crate::config::Config;
use crate::groups::coordinator::Coordinator;
use crate::log::PartitionLog;
use crate::storage::Storage;

/// Upper bound on partitions per topic. Each partition is a branch walked at startup,
/// and the CreateTopic event is replayed forever, so an absurd count must never be recorded.
pub const MAX_PARTITIONS: i32 = 1000;

pub struct TopicState {
    pub topic_id: Uuid,
    /// The topic whose sentinels this topic's branches start at: its own name unless
    /// it is a fork.
    pub root: String,
    pub partitions: Vec<Arc<dyn PartitionLog>>,
}

impl TopicState {
    pub fn partition(&self, index: i32) -> Option<&Arc<dyn PartitionLog>> {
        usize::try_from(index)
            .ok()
            .and_then(|i| self.partitions.get(i))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateTopicError {
    #[error("topic already exists")]
    AlreadyExists,
    #[error("invalid topic name: {0}")]
    InvalidName(String),
    #[error("invalid partition count {0}")]
    InvalidPartitions(i32),
    #[error("storage error: {0}")]
    Storage(String),
}

impl CreateTopicError {
    pub fn code(&self) -> i16 {
        match self {
            CreateTopicError::AlreadyExists => ResponseError::TopicAlreadyExists.code(),
            CreateTopicError::InvalidName(_) => ResponseError::InvalidTopicException.code(),
            CreateTopicError::InvalidPartitions(_) => ResponseError::InvalidPartitions.code(),
            CreateTopicError::Storage(_) => ResponseError::KafkaStorageError.code(),
        }
    }
}

/// Kafka's rules (`[a-zA-Z0-9._-]{1,249}`, not `.`/`..`) plus what Git refs cannot hold.
pub fn validate_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 249 {
        return Err("must be 1-249 characters".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    {
        return Err("may only contain ASCII letters, digits, '.', '_' and '-'".into());
    }
    if name.starts_with('.') || name.contains("..") || name.ends_with(".lock") {
        return Err("cannot start with '.', contain '..' or end in '.lock' (Git ref rules)".into());
    }
    Ok(())
}

fn validate_partitions(partitions: i32) -> Result<(), CreateTopicError> {
    if (1..=MAX_PARTITIONS).contains(&partitions) {
        Ok(())
    } else {
        Err(CreateTopicError::InvalidPartitions(partitions))
    }
}

/// Producer ids are recorded in blocks so a restart never hands one out twice.
const PRODUCER_ID_BLOCK: i64 = 1000;

struct ProducerIds {
    next: i64,
    /// Ids below this are covered by a recorded AllocateProducerIds event.
    allocated_up_to: i64,
}

pub struct Broker {
    pub config: Config,
    storage: Arc<dyn Storage>,
    topics: RwLock<BTreeMap<String, Arc<TopicState>>>,
    appended: watch::Sender<u64>,
    producer_ids: tokio::sync::Mutex<ProducerIds>,
    known_groups: Vec<String>,
    pub coordinator: Arc<Coordinator>,
    pub(crate) idempotence: IdempotenceCache,
}

impl Broker {
    pub async fn start(config: Config, storage: Arc<dyn Storage>) -> anyhow::Result<Arc<Self>> {
        let loaded = storage.load().await?;
        let mut topics = BTreeMap::new();
        for t in loaded.topics {
            topics.insert(
                t.name,
                Arc::new(TopicState {
                    topic_id: t.topic_id,
                    root: t.root,
                    partitions: t.partitions,
                }),
            );
        }
        let coordinator =
            Coordinator::start(config.group_initial_rebalance_delay, loaded.groups.clone());
        Ok(Arc::new(Broker {
            config,
            storage,
            coordinator,
            idempotence: IdempotenceCache::default(),
            topics: RwLock::new(topics),
            appended: watch::channel(0).0,
            producer_ids: tokio::sync::Mutex::new(ProducerIds {
                next: loaded.producer_id_high,
                allocated_up_to: loaded.producer_id_high,
            }),
            known_groups: loaded.groups,
        }))
    }

    /// Groups that had committed offsets when the broker started, sorted.
    pub fn known_groups(&self) -> Vec<String> {
        self.known_groups.clone()
    }

    /// A producer id never handed out before, even across restarts.
    pub async fn next_producer_id(&self) -> anyhow::Result<i64> {
        let mut ids = self.producer_ids.lock().await;
        if ids.next >= ids.allocated_up_to {
            let up_to = ids.allocated_up_to + PRODUCER_ID_BLOCK;
            self.storage.allocate_producer_ids(up_to).await?;
            ids.allocated_up_to = up_to;
        }
        let id = ids.next;
        ids.next += 1;
        Ok(id)
    }

    pub async fn topic(&self, name: &str) -> Option<Arc<TopicState>> {
        self.topics.read().await.get(name).cloned()
    }

    pub async fn topics(&self) -> Vec<(String, Arc<TopicState>)> {
        self.topics
            .read()
            .await
            .iter()
            .map(|(n, t)| (n.clone(), t.clone()))
            .collect()
    }

    pub async fn validate_new_topic(
        &self,
        name: &str,
        partitions: i32,
    ) -> Result<(), CreateTopicError> {
        validate_topic_name(name).map_err(CreateTopicError::InvalidName)?;
        validate_partitions(partitions)?;
        if self.topics.read().await.contains_key(name) {
            return Err(CreateTopicError::AlreadyExists);
        }
        Ok(())
    }

    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
    ) -> Result<Arc<TopicState>, CreateTopicError> {
        validate_topic_name(name).map_err(CreateTopicError::InvalidName)?;
        validate_partitions(partitions)?;
        // Hold the write lock across storage so two creators cannot race.
        let mut topics = self.topics.write().await;
        if topics.contains_key(name) {
            return Err(CreateTopicError::AlreadyExists);
        }
        let topic_id = Uuid::new_v4();
        let logs = self
            .storage
            .create_topic(name, topic_id, partitions)
            .await
            .map_err(|e| CreateTopicError::Storage(format!("{e:#}")))?;
        let state = Arc::new(TopicState {
            topic_id,
            root: name.to_string(),
            partitions: logs,
        });
        topics.insert(name.to_string(), state.clone());
        tracing::info!(topic = name, partitions, "created topic");
        Ok(state)
    }

    pub async fn get_or_auto_create(
        &self,
        name: &str,
    ) -> Result<Option<Arc<TopicState>>, CreateTopicError> {
        if let Some(t) = self.topic(name).await {
            return Ok(Some(t));
        }
        validate_topic_name(name).map_err(CreateTopicError::InvalidName)?;
        if !self.config.auto_create_topics {
            return Ok(None);
        }
        match self
            .create_topic(name, self.config.default_partitions)
            .await
        {
            Ok(t) => Ok(Some(t)),
            Err(CreateTopicError::AlreadyExists) => Ok(self.topic(name).await),
            Err(e) => Err(e),
        }
    }

    pub fn notify_appended(&self) {
        self.appended.send_modify(|n| *n = n.wrapping_add(1));
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.appended.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::store::GitStore;
    use crate::storage::{GitStorage, MemStorage};

    async fn mem_broker(auto_create: bool) -> Arc<Broker> {
        let mut config = Config::for_tests();
        config.auto_create_topics = auto_create;
        Broker::start(config, Arc::new(MemStorage)).await.unwrap()
    }

    #[tokio::test]
    async fn creates_topics_and_rejects_duplicates() {
        let broker = mem_broker(true).await;
        let t = broker.create_topic("orders", 3).await.unwrap();
        assert_eq!(t.partitions.len(), 3);
        assert!(t.partition(2).is_some() && t.partition(3).is_none() && t.partition(-1).is_none());
        assert!(matches!(
            broker.create_topic("orders", 1).await,
            Err(CreateTopicError::AlreadyExists)
        ));
        assert!(matches!(
            broker.create_topic("x", 0).await,
            Err(CreateTopicError::InvalidPartitions(0))
        ));
        assert_eq!(broker.topics().await.len(), 1);
    }

    #[tokio::test]
    async fn producer_ids_keep_increasing_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let first: Vec<i64> = {
            let store = Arc::new(GitStore::open_or_init(&path).unwrap());
            let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
                .await
                .unwrap();
            let mut ids = Vec::new();
            for _ in 0..3 {
                ids.push(broker.next_producer_id().await.unwrap());
            }
            ids
        };
        assert_eq!(first, vec![0, 1, 2]);
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
            .await
            .unwrap();
        let next = broker.next_producer_id().await.unwrap();
        assert!(next > 2, "reused producer id {next}");
    }

    #[tokio::test]
    async fn groups_with_committed_offsets_are_known_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let store = Arc::new(GitStore::open_or_init(&path).unwrap());
            let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
                .await
                .unwrap();
            let t = broker.create_topic("orders", 2).await.unwrap();
            t.partitions[1].commit_offset("my group", 0).await.unwrap();
            t.partitions[0].commit_offset("billing", 0).await.unwrap();
        }
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
            .await
            .unwrap();
        assert_eq!(
            broker.known_groups(),
            vec!["billing".to_string(), "my group".to_string()]
        );
    }

    #[tokio::test]
    async fn absurd_partition_counts_are_rejected_before_touching_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
            .await
            .unwrap();
        for n in [MAX_PARTITIONS + 1, i32::MAX] {
            assert!(matches!(
                broker.create_topic("huge", n).await,
                Err(CreateTopicError::InvalidPartitions(_))
            ));
            assert!(matches!(
                broker.validate_new_topic("huge", n).await,
                Err(CreateTopicError::InvalidPartitions(_))
            ));
        }
        // nothing was recorded, so a restart is unaffected
        drop(broker);
        let store = GitStore::open_or_init(&path).unwrap();
        assert!(crate::git::meta::replay(&store).unwrap().is_empty());

        // the cap itself is allowed
        let broker = mem_broker(true).await;
        broker.create_topic("max", MAX_PARTITIONS).await.unwrap();
    }

    #[tokio::test]
    async fn creating_over_a_foreign_branch_fails_without_recording_the_topic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        {
            let store = Arc::new(GitStore::open_or_init(&path).unwrap());
            // someone hand-makes refs/heads/orders/0 from another topic's history
            let other = crate::log::git::GitLog::open_or_create(store.clone(), "payments", 0)
                .await
                .unwrap();
            drop(other);
            let head = store.ref_target("refs/heads/payments/0").unwrap().unwrap();
            store
                .cas_ref("refs/heads/orders/0", None, head, "hand-made")
                .unwrap();
            let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
                .await
                .unwrap();
            let err = broker.create_topic("orders", 1).await.err();
            assert!(matches!(err, Some(CreateTopicError::Storage(_))), "{err:?}");
        }
        // the failed create left no metadata behind, so the broker still starts
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
            .await
            .unwrap();
        assert!(broker.topic("orders").await.is_none());
    }

    #[tokio::test]
    async fn rejects_names_kafka_or_git_cannot_hold() {
        let broker = mem_broker(true).await;
        let too_long = "a".repeat(250);
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "a..b",
            "x.lock",
            "has space",
            "a/b",
            "ünï",
            too_long.as_str(),
        ] {
            let err = broker.create_topic(bad, 1).await.err();
            assert!(
                matches!(err, Some(CreateTopicError::InvalidName(_))),
                "{bad:?} -> {err:?}"
            );
        }
        for good in ["orders", "my.topic-v2_final", "a"] {
            broker.create_topic(good, 1).await.unwrap();
        }
    }

    #[tokio::test]
    async fn auto_create_honours_the_config() {
        let on = mem_broker(true).await;
        let t = on.get_or_auto_create("fresh").await.unwrap().unwrap();
        assert_eq!(t.partitions.len(), 1);
        let off = mem_broker(false).await;
        assert!(off.get_or_auto_create("fresh").await.unwrap().is_none());
        assert!(matches!(
            on.get_or_auto_create("bad name").await,
            Err(CreateTopicError::InvalidName(_))
        ));
    }

    #[tokio::test]
    async fn notify_appended_wakes_subscribers() {
        let broker = mem_broker(true).await;
        let mut rx = broker.subscribe();
        broker.notify_appended();
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.changed())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn git_backed_topics_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let id = {
            let store = Arc::new(GitStore::open_or_init(&path).unwrap());
            let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
                .await
                .unwrap();
            let t = broker.create_topic("orders", 2).await.unwrap();
            t.partitions[1]
                .append("p", vec![crate::record::Record::text(1, "kept")])
                .await
                .unwrap();
            t.topic_id
        };
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let broker = Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
            .await
            .unwrap();
        let t = broker.topic("orders").await.unwrap();
        assert_eq!(t.topic_id, id);
        assert_eq!(t.partitions.len(), 2);
        assert_eq!(t.partitions[1].high_watermark(), 1);
    }
}
