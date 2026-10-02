//! The only place that touches the Git repository. Every method is blocking;
//! async callers wrap calls in `tokio::task::spawn_blocking`.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use gix::ObjectId;
use gix::refs::Target;
use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

pub struct GitStore {
    repo: gix::ThreadSafeRepository,
    path: PathBuf,
    empty_tree: ObjectId,
    _lock: File,
}

impl GitStore {
    pub fn open_or_init(path: &Path) -> Result<Self> {
        let repo = if path.join("HEAD").exists() {
            gix::open(path).with_context(|| format!("opening {}", path.display()))?
        } else {
            gix::init_bare(path).with_context(|| format!("creating {}", path.display()))?
        };
        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path.join("kommit.lock"))?;
        lock.try_lock()
            .map_err(|_| anyhow!("another kommit broker is using {}", path.display()))?;
        // Commits point at the empty tree, so it must exist for fsck.
        let empty_tree = repo.write_object(gix::objs::Tree::empty())?.detach();
        Ok(GitStore {
            repo: repo.into_sync(),
            path: path.to_path_buf(),
            empty_tree,
            _lock: lock,
        })
    }

    pub fn empty_tree(&self) -> ObjectId {
        self.empty_tree
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write_commit(&self, commit: &gix::objs::Commit) -> Result<ObjectId> {
        Ok(self.repo.to_thread_local().write_object(commit)?.detach())
    }

    pub fn with_commit<T>(
        &self,
        id: ObjectId,
        f: impl FnOnce(&gix::objs::CommitRef<'_>) -> Result<T>,
    ) -> Result<T> {
        let repo = self.repo.to_thread_local();
        let commit = repo.find_commit(id)?;
        let decoded = commit.decode()?;
        f(&decoded)
    }

    pub fn ref_target(&self, name: &str) -> Result<Option<ObjectId>> {
        let repo = self.repo.to_thread_local();
        match repo.try_find_reference(name)? {
            Some(r) => Ok(Some(r.into_fully_peeled_id()?.detach())),
            None => Ok(None),
        }
    }

    /// Moves `name` to `new` only if it currently points at `expected` (`None`: must not exist).
    pub fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
        message: &str,
    ) -> Result<()> {
        let expected = match expected {
            Some(id) => PreviousValue::MustExistAndMatch(Target::Object(id)),
            None => PreviousValue::MustNotExist,
        };
        self.repo.to_thread_local().edit_reference(RefEdit {
            change: Change::Update {
                log: LogChange {
                    message: message.into(),
                    ..Default::default()
                },
                expected,
                new: Target::Object(new),
            },
            name: name.try_into()?,
            deref: false,
        })?;
        Ok(())
    }

    /// The first-parent chain ending at `head`, oldest first.
    pub fn first_parent_chain(&self, head: ObjectId) -> Result<Vec<ObjectId>> {
        let repo = self.repo.to_thread_local();
        let mut chain = Vec::new();
        let mut current = Some(head);
        while let Some(id) = current {
            chain.push(id);
            current = repo
                .find_commit(id)?
                .parent_ids()
                .next()
                .map(|p| p.detach());
        }
        chain.reverse();
        Ok(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::encode::{record_to_commit, sentinel_commit};
    use crate::git::testutil::git;
    use crate::record::Record;
    use bytes::Bytes;

    fn store() -> (tempfile::TempDir, GitStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = GitStore::open_or_init(&dir.path().join("data.git")).unwrap();
        (dir, store)
    }

    #[test]
    fn init_creates_a_bare_repo_that_reopens() {
        let (dir, store) = store();
        assert!(dir.path().join("data.git/HEAD").exists());
        drop(store);
        GitStore::open_or_init(&dir.path().join("data.git")).unwrap();
    }

    #[test]
    fn second_broker_on_the_same_repo_is_refused() {
        let (dir, _held) = store();
        let err = GitStore::open_or_init(&dir.path().join("data.git"))
            .err()
            .unwrap();
        assert!(err.to_string().contains("another kommit broker"), "{err:#}");
    }

    #[test]
    fn commits_chain_and_refs_move_with_compare_and_swap() {
        let (_dir, s) = store();
        let root = s
            .write_commit(&sentinel_commit("t", 0, s.empty_tree(), 0))
            .unwrap();
        let a = s
            .write_commit(&record_to_commit(
                &Record::text(1, "a"),
                "p",
                root,
                s.empty_tree(),
            ))
            .unwrap();
        let b = s
            .write_commit(&record_to_commit(
                &Record::text(2, "b"),
                "p",
                a,
                s.empty_tree(),
            ))
            .unwrap();

        assert_eq!(s.ref_target("refs/heads/t/0").unwrap(), None);
        s.cas_ref("refs/heads/t/0", None, root, "create").unwrap();
        s.cas_ref("refs/heads/t/0", Some(root), b, "produce")
            .unwrap();
        assert_eq!(s.ref_target("refs/heads/t/0").unwrap(), Some(b));
        assert_eq!(s.first_parent_chain(b).unwrap(), vec![root, a, b]);

        // stale expectation fails and leaves the ref alone
        assert!(s.cas_ref("refs/heads/t/0", Some(root), a, "stale").is_err());
        // creating an existing ref fails
        assert!(s.cas_ref("refs/heads/t/0", None, a, "dup").is_err());
        assert_eq!(s.ref_target("refs/heads/t/0").unwrap(), Some(b));

        let msg = s.with_commit(b, |c| Ok(c.message.to_string())).unwrap();
        assert_eq!(msg, "b");
    }

    #[test]
    fn hostile_records_keep_the_repo_fsck_clean_and_readable_by_git() {
        let (dir, s) = store();
        let root = s
            .write_commit(&sentinel_commit("t", 0, s.empty_tree(), 0))
            .unwrap();
        let hostile = Record {
            timestamp_ms: -1,
            key: Some(Bytes::from_static(b"\x00key with spaces\n")),
            value: Some(Bytes::from_static(b"\xff\x00binary")),
            headers: vec![("h 1".into(), None), ("h2".into(), Some(Bytes::new()))],
        };
        let a = s
            .write_commit(&record_to_commit(
                &hostile,
                "evil<client>\nid",
                root,
                s.empty_tree(),
            ))
            .unwrap();
        let b = s
            .write_commit(&record_to_commit(
                &Record::text(i64::MAX, "hello git"),
                "",
                a,
                s.empty_tree(),
            ))
            .unwrap();
        s.cas_ref("refs/heads/t/0", None, b, "test").unwrap();

        let repo = dir.path().join("data.git");
        git(&repo, &["fsck", "--strict", "--no-dangling"]);
        let log = git(&repo, &["log", "--format=%an|%s", "t/0"]);
        assert_eq!(log.lines().next().unwrap(), "anonymous|hello git");
        assert!(log.contains("evil_client__id|"));
    }
}
