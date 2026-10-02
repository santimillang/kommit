use std::sync::Arc;

use anyhow::{Result, bail};
use uuid::Uuid;

use crate::git::meta::{self, MetaEvent};
use crate::git::store::GitStore;
use crate::groups::group_from_ref_component;
use crate::log::PartitionLog;
use crate::log::git::{GitLog, partition_ref};
use crate::log::mem::MemLog;
use crate::record::now_ms;

pub struct LoadedTopic {
    pub name: String,
    pub topic_id: Uuid,
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

    async fn allocate_producer_ids(&self, _up_to: i64) -> Result<()> {
        Ok(())
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
    async fn load(&self) -> Result<Loaded> {
        let store = self.store.clone();
        let (events, group_refs) = tokio::task::spawn_blocking(move || -> Result<_> {
            Ok((
                meta::replay(&store)?,
                store.refs_with_prefix("refs/groups/")?,
            ))
        })
        .await??;
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
                        name,
                        topic_id,
                        partitions,
                    });
                }
                MetaEvent::AllocateProducerIds { up_to } => {
                    loaded.producer_id_high = loaded.producer_id_high.max(up_to);
                }
            }
        }
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
        self.open_partitions(name, partitions).await
    }

    async fn allocate_producer_ids(&self, up_to: i64) -> Result<()> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            meta::append(&store, &MetaEvent::AllocateProducerIds { up_to }, now_ms())
        })
        .await?
    }
}
