use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::git::meta::{self, MetaEvent};
use crate::git::store::GitStore;
use crate::log::PartitionLog;
use crate::log::git::GitLog;
use crate::log::mem::MemLog;
use crate::record::now_ms;

pub struct LoadedTopic {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: Vec<Arc<dyn PartitionLog>>,
}

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn load_topics(&self) -> Result<Vec<LoadedTopic>>;
    async fn create_topic(
        &self,
        name: &str,
        topic_id: Uuid,
        partitions: i32,
    ) -> Result<Vec<Arc<dyn PartitionLog>>>;
}

pub struct MemStorage;

#[async_trait::async_trait]
impl Storage for MemStorage {
    async fn load_topics(&self) -> Result<Vec<LoadedTopic>> {
        Ok(Vec::new())
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
                GitLog::open_or_create(self.store.clone(), name, p).await?,
            ));
        }
        Ok(logs)
    }
}

#[async_trait::async_trait]
impl Storage for GitStorage {
    async fn load_topics(&self) -> Result<Vec<LoadedTopic>> {
        let store = self.store.clone();
        let events = tokio::task::spawn_blocking(move || meta::replay(&store)).await??;
        let mut topics = Vec::new();
        for event in events {
            match event {
                MetaEvent::CreateTopic {
                    name,
                    partitions,
                    topic_id,
                } => {
                    let partitions = self.open_partitions(&name, partitions).await?;
                    topics.push(LoadedTopic {
                        name,
                        topic_id,
                        partitions,
                    });
                }
            }
        }
        Ok(topics)
    }

    /// The metadata commit is the commit point: it is written before the branches.
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
        tokio::task::spawn_blocking(move || meta::append(&store, &event, now_ms())).await??;
        self.open_partitions(name, partitions).await
    }
}
