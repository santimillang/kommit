//! Cluster metadata as a commit chain, like KRaft's `__cluster_metadata`, but in Git.

use anyhow::Result;
use gix::bstr::ByteSlice;
use serde::{Deserialize, Serialize};

use crate::git::encode::kommit_signature;
use crate::git::store::GitStore;

pub const META_REF: &str = "refs/kommit/meta";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum MetaEvent {
    CreateTopic {
        name: String,
        partitions: i32,
        topic_id: uuid::Uuid,
    },
    /// Producer ids below `up_to` may have been handed out; a restart resumes there.
    AllocateProducerIds { up_to: i64 },
}

pub fn append(store: &GitStore, event: &MetaEvent, now_ms: i64) -> Result<()> {
    let parent = store.ref_target(META_REF)?;
    let sig = kommit_signature("kommit", now_ms);
    let commit = gix::objs::Commit {
        tree: store.empty_tree(),
        parents: parent.into_iter().collect(),
        author: sig.clone(),
        committer: sig,
        encoding: None,
        message: toml::to_string(event)?.into(),
        extra_headers: vec![],
    };
    let id = store.write_commit(&commit)?;
    store.cas_ref(META_REF, parent, id, "kommit: metadata event")
}

pub fn replay(store: &GitStore) -> Result<Vec<MetaEvent>> {
    let Some(head) = store.ref_target(META_REF)? else {
        return Ok(Vec::new());
    };
    store
        .first_parent_chain(head)?
        .into_iter()
        .map(|id| store.with_commit(id, |c| Ok(toml::from_str(c.message.to_str()?)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::testutil::git;

    fn create(name: &str, partitions: i32) -> MetaEvent {
        MetaEvent::CreateTopic {
            name: name.into(),
            partitions,
            topic_id: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn empty_repo_replays_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = GitStore::open_or_init(&dir.path().join("d.git")).unwrap();
        assert_eq!(replay(&store).unwrap(), vec![]);
    }

    #[test]
    fn events_replay_in_order_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d.git");
        let (a, b) = (
            create("orders", 3),
            MetaEvent::AllocateProducerIds { up_to: 1000 },
        );
        {
            let store = GitStore::open_or_init(&path).unwrap();
            append(&store, &a, 1_000).unwrap();
            append(&store, &b, 2_000).unwrap();
        }
        let store = GitStore::open_or_init(&path).unwrap();
        assert_eq!(replay(&store).unwrap(), vec![a, b]);

        let log = git(&path, &["log", "--format=%B", META_REF]);
        assert!(log.contains("event = \"CreateTopic\""), "{log}");
        assert!(log.contains("name = \"orders\""), "{log}");
    }
}
