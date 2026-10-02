use std::collections::BTreeMap;
use std::sync::Arc;

use kafka_protocol::ResponseError;
use tokio::sync::{RwLock, watch};
use uuid::Uuid;

use crate::config::Config;
use crate::log::PartitionLog;
use crate::storage::Storage;

pub struct TopicState {
    pub topic_id: Uuid,
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

pub struct Broker {
    pub config: Config,
    storage: Arc<dyn Storage>,
    topics: RwLock<BTreeMap<String, Arc<TopicState>>>,
    appended: watch::Sender<u64>,
}

impl Broker {
    pub async fn start(config: Config, storage: Arc<dyn Storage>) -> anyhow::Result<Arc<Self>> {
        let mut topics = BTreeMap::new();
        for t in storage.load_topics().await? {
            topics.insert(
                t.name,
                Arc::new(TopicState {
                    topic_id: t.topic_id,
                    partitions: t.partitions,
                }),
            );
        }
        Ok(Arc::new(Broker {
            config,
            storage,
            topics: RwLock::new(topics),
            appended: watch::channel(0).0,
        }))
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
        if partitions < 1 {
            return Err(CreateTopicError::InvalidPartitions(partitions));
        }
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
        if partitions < 1 {
            return Err(CreateTopicError::InvalidPartitions(partitions));
        }
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
