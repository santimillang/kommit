//! Topic branching (M3): which commit each partition of a fork starts from.
//!
//! A fork point `n` means the fork shares the source's records `0..n`: its high
//! watermark starts at `n`, and its branch points at the source's commit for offset
//! `n - 1` (the shared sentinel when `n` is 0).

use std::collections::BTreeMap;
use std::sync::Arc;

pub use crate::git::meta::GroupOffset;
use crate::log::{LogError, PartitionLog};
use crate::record::Offset;

pub const BRANCH_FROM: &str = "kommit.branch.from";
pub const BRANCH_AT: &str = "kommit.branch.at";
pub const BRANCH_GROUPS: &str = "kommit.branch.groups";

#[derive(Debug, Clone, PartialEq)]
pub enum BranchAt {
    /// Every partition's current high watermark.
    Head,
    /// An explicit fork point for every partition of the source.
    Offsets(BTreeMap<i32, Offset>),
    /// Milliseconds since the epoch. Per partition, the first offset whose record
    /// timestamp is at or after this, like ListOffsets.
    Timestamp(i64),
}

/// Which consumer groups' committed offsets the fork takes along (spec §7.2).
#[derive(Debug, Clone, PartialEq)]
pub enum BranchGroups {
    None,
    /// Every group with a committed offset on any source partition.
    All,
    Named(Vec<String>),
}

impl BranchGroups {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("none") {
            return Ok(BranchGroups::None);
        }
        if s.eq_ignore_ascii_case("all") {
            return Ok(BranchGroups::All);
        }
        let mut names: Vec<String> = Vec::new();
        for name in s.split(',').map(str::trim) {
            if name.is_empty() {
                return Err(format!(
                    "{BRANCH_GROUPS}={s:?}: empty group id; expected `none`, `all` or a \
                     comma-separated list of groups"
                ));
            }
            if names.iter().any(|n| n == name) {
                return Err(format!("{BRANCH_GROUPS}: group {name} is listed twice"));
            }
            names.push(name.to_string());
        }
        Ok(BranchGroups::Named(names))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BranchSpec {
    pub from: String,
    pub at: BranchAt,
    pub groups: BranchGroups,
}

fn expected_forms(s: &str) -> String {
    format!(
        "{BRANCH_AT}={s:?}: expected `head`, offsets for every partition like `0:42,1:17`, \
         or an RFC 3339 timestamp like `2026-10-01T12:00:00Z`"
    )
}

impl BranchAt {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("head") {
            return Ok(BranchAt::Head);
        }
        if let Some(offsets) = parse_offsets(s)? {
            return Ok(BranchAt::Offsets(offsets));
        }
        s.parse::<jiff::Timestamp>()
            .map(|t| BranchAt::Timestamp(t.as_millisecond()))
            .map_err(|_| expected_forms(s))
    }
}

/// `Ok(None)` if `s` is not shaped like `p:o,p:o`, so it can be tried as a timestamp.
/// Only plain numbers count (all digits, no leading zero), so a clock time like `12:00`
/// or a timestamp like `2026-10-01T12:00:00Z` is never read as offsets.
fn parse_offsets(s: &str) -> Result<Option<BTreeMap<i32, Offset>>, String> {
    let digits = |x: &str| {
        !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit()) && (x == "0" || !x.starts_with('0'))
    };
    let pairs: Option<Vec<(&str, &str)>> = s
        .split(',')
        .map(|part| {
            let (p, o) = part.split_once(':')?;
            let (p, o) = (p.trim(), o.trim());
            (digits(p) && digits(o)).then_some((p, o))
        })
        .collect();
    let Some(pairs) = pairs else {
        return Ok(None);
    };
    let mut out = BTreeMap::new();
    for (p, o) in pairs {
        let p: i32 = p
            .parse()
            .map_err(|_| format!("partition {p} is out of range"))?;
        let o: Offset = o
            .parse()
            .map_err(|_| format!("offset {o} is out of range"))?;
        if out.insert(p, o).is_some() {
            return Err(format!("{BRANCH_AT}: partition {p} is listed twice"));
        }
    }
    Ok(Some(out))
}

impl BranchSpec {
    /// Reads the `kommit.*` entries of a CreateTopics request. `Ok(None)` means it is a
    /// plain topic. Other configs are ignored, as they always have been.
    pub fn from_configs<'a>(
        configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    ) -> Result<Option<Self>, String> {
        let (mut from, mut at, mut groups): (Option<&str>, Option<&str>, Option<&str>) =
            (None, None, None);
        for (name, value) in configs {
            let slot = match name {
                BRANCH_FROM => &mut from,
                BRANCH_AT => &mut at,
                BRANCH_GROUPS => &mut groups,
                n if n.starts_with("kommit.") => {
                    return Err(format!(
                        "unknown config {n}; kommit understands {BRANCH_FROM}, {BRANCH_AT} \
                         and {BRANCH_GROUPS}"
                    ));
                }
                _ => continue,
            };
            let Some(value) = value else {
                return Err(format!("{name} needs a value"));
            };
            if slot.replace(value).is_some() {
                return Err(format!("{name} is given twice"));
            }
        }
        let Some(from) = from else {
            return match at.or(groups) {
                None => Ok(None),
                Some(_) => Err(format!(
                    "{BRANCH_AT} and {BRANCH_GROUPS} need {BRANCH_FROM}"
                )),
            };
        };
        Ok(Some(BranchSpec {
            from: from.to_string(),
            at: at
                .map(BranchAt::parse)
                .transpose()?
                .unwrap_or(BranchAt::Head),
            groups: groups
                .map(BranchGroups::parse)
                .transpose()?
                .unwrap_or(BranchGroups::None),
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// The spec does not fit the source topic.
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Log(#[from] LogError),
}

/// The fork point of every source partition, in partition order.
pub async fn resolve(
    at: &BranchAt,
    source: &[Arc<dyn PartitionLog>],
) -> Result<Vec<Offset>, ResolveError> {
    // A faulted branch was moved behind kommit's back, so Git no longer holds what its
    // consumers were shown. Forking it would copy that, silently.
    for (p, log) in source.iter().enumerate() {
        if let Some(fault) = log.fault() {
            return Err(LogError::Faulted(format!(
                "source partition {p} is faulted and cannot be branched: {fault}"
            ))
            .into());
        }
    }
    match at {
        BranchAt::Head => Ok(source.iter().map(|log| log.high_watermark()).collect()),
        BranchAt::Offsets(map) => {
            if let Some(p) = map
                .keys()
                .find(|p| !usize::try_from(**p).is_ok_and(|i| i < source.len()))
            {
                return Err(ResolveError::Invalid(format!(
                    "partition {p} does not exist in the source topic ({} partition(s))",
                    source.len()
                )));
            }
            let mut out = Vec::with_capacity(source.len());
            for (p, log) in source.iter().enumerate() {
                let Some(&offset) = map.get(&(p as i32)) else {
                    return Err(ResolveError::Invalid(format!(
                        "no offset for partition {p}: list every partition, like 0:42,1:17"
                    )));
                };
                let hw = log.high_watermark();
                if offset > hw {
                    return Err(ResolveError::Invalid(format!(
                        "partition {p} has {hw} record(s); cannot branch at offset {offset}"
                    )));
                }
                out.push(offset);
            }
            Ok(out)
        }
        BranchAt::Timestamp(ts) => {
            let mut out = Vec::with_capacity(source.len());
            for log in source {
                out.push(match log.offset_for_timestamp(*ts).await? {
                    Some((offset, _)) => offset,
                    None => log.high_watermark(),
                });
            }
            Ok(out)
        }
    }
}

/// The committed offsets a fork takes along: per chosen group and source partition p
/// with a commit `c`, `min(c, at[p])`. A group that had read past the fork point is
/// caught up on the fork. Sorted by group, then partition.
pub async fn resolve_groups(
    groups: &BranchGroups,
    source: &[Arc<dyn PartitionLog>],
    at: &[Offset],
) -> Result<Vec<GroupOffset>, ResolveError> {
    let names: Vec<String> = match groups {
        BranchGroups::None => return Ok(Vec::new()),
        BranchGroups::All => {
            let mut all = Vec::new();
            for log in source {
                all.extend(log.committed_groups().await?);
            }
            all.sort();
            all.dedup();
            all
        }
        BranchGroups::Named(names) => {
            let mut names = names.clone();
            names.sort();
            names
        }
    };
    let mut out = Vec::new();
    for group in names {
        let mut found = false;
        for (p, (log, &n)) in source.iter().zip(at).enumerate() {
            if let Some(c) = log.committed_offset(&group).await? {
                found = true;
                out.push(GroupOffset {
                    group: group.clone(),
                    partition: p as i32,
                    offset: c.min(n),
                });
            }
        }
        if !found {
            return Err(ResolveError::Invalid(format!(
                "{BRANCH_GROUPS}: group {group} has no committed offsets on the source topic"
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::mem::MemLog;
    use crate::record::Record;

    fn offsets(pairs: &[(i32, Offset)]) -> BranchAt {
        BranchAt::Offsets(pairs.iter().copied().collect())
    }

    #[test]
    fn parses_head_offsets_and_timestamps() {
        assert_eq!(BranchAt::parse("head").unwrap(), BranchAt::Head);
        assert_eq!(BranchAt::parse(" head ").unwrap(), BranchAt::Head);
        assert_eq!(BranchAt::parse("HEAD").unwrap(), BranchAt::Head);
        assert_eq!(BranchAt::parse("0:42").unwrap(), offsets(&[(0, 42)]));
        assert_eq!(
            BranchAt::parse("0:42, 1:17").unwrap(),
            offsets(&[(0, 42), (1, 17)])
        );
        // 2026-10-01T12:00:00Z
        let noon = 1_790_856_000_000;
        assert_eq!(
            BranchAt::parse("2026-10-01T12:00:00Z").unwrap(),
            BranchAt::Timestamp(noon)
        );
        assert_eq!(
            BranchAt::parse("2026-10-01T12:00:00.250Z").unwrap(),
            BranchAt::Timestamp(noon + 250)
        );
        assert_eq!(
            BranchAt::parse("2026-10-01T14:00:00+02:00").unwrap(),
            BranchAt::Timestamp(noon)
        );
    }

    #[test]
    fn rejects_ambiguous_or_malformed_points() {
        for bad in [
            "",
            "tail",
            "12:00",
            "2026-10-01T12:00:00",
            "0:-1",
            "-1",
            "0:1,x",
        ] {
            let err = BranchAt::parse(bad).unwrap_err();
            assert!(err.contains("head"), "{bad:?}: {err}");
        }
        let err = BranchAt::parse("0:1,0:2").unwrap_err();
        assert!(err.contains("twice"), "{err}");
        let err = BranchAt::parse("0:99999999999999999999").unwrap_err();
        assert!(err.contains("out of range"), "{err}");
    }

    #[test]
    fn reads_branch_configs() {
        let none: [(&str, Option<&str>); 1] = [("retention.ms", Some("1000"))];
        assert_eq!(BranchSpec::from_configs(none).unwrap(), None);

        let spec = BranchSpec::from_configs([(BRANCH_FROM, Some("orders"))])
            .unwrap()
            .unwrap();
        assert_eq!(
            spec,
            BranchSpec {
                from: "orders".into(),
                at: BranchAt::Head,
                groups: BranchGroups::None,
            }
        );

        let spec = BranchSpec::from_configs([
            (BRANCH_AT, Some("0:1")),
            ("cleanup.policy", Some("delete")),
            (BRANCH_FROM, Some("orders")),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(spec.at, offsets(&[(0, 1)]));

        let groups = |v: &'static str| {
            BranchSpec::from_configs([(BRANCH_FROM, Some("orders")), (BRANCH_GROUPS, Some(v))])
                .map(|s| s.unwrap().groups)
        };
        assert_eq!(groups("none").unwrap(), BranchGroups::None);
        assert_eq!(groups("ALL").unwrap(), BranchGroups::All);
        assert_eq!(
            groups("billing, audit").unwrap(),
            BranchGroups::Named(vec!["billing".into(), "audit".into()])
        );
        assert!(groups("billing,,audit").unwrap_err().contains("empty"));
        assert!(groups("a,a").unwrap_err().contains("twice"));

        for (configs, needle) in [
            (
                vec![(BRANCH_GROUPS, Some("all"))],
                "need kommit.branch.from",
            ),
            (vec![(BRANCH_AT, Some("head"))], "need kommit.branch.from"),
            (vec![(BRANCH_FROM, None)], "needs a value"),
            (
                vec![(BRANCH_FROM, Some("a")), (BRANCH_FROM, Some("b"))],
                "twice",
            ),
            (vec![("kommit.branch.form", Some("a"))], "unknown config"),
            (
                vec![(BRANCH_FROM, Some("a")), (BRANCH_AT, Some("nope"))],
                "head",
            ),
        ] {
            let err = BranchSpec::from_configs(configs).unwrap_err();
            assert!(err.contains(needle), "{needle}: {err}");
        }
    }

    async fn log_with(timestamps: &[i64]) -> Arc<dyn PartitionLog> {
        let log = MemLog::default();
        let recs = timestamps.iter().map(|ts| Record::text(*ts, "x")).collect();
        log.append("p", recs).await.unwrap();
        Arc::new(log)
    }

    #[tokio::test]
    async fn resolves_fork_points_per_partition() {
        let source = vec![log_with(&[100, 200, 300]).await, log_with(&[150]).await];

        assert_eq!(resolve(&BranchAt::Head, &source).await.unwrap(), vec![3, 1]);
        assert_eq!(
            resolve(&offsets(&[(0, 0), (1, 1)]), &source).await.unwrap(),
            vec![0, 1]
        );
        // first record at or after ts; none that late means the head
        assert_eq!(
            resolve(&BranchAt::Timestamp(200), &source).await.unwrap(),
            vec![1, 1]
        );
        assert_eq!(
            resolve(&BranchAt::Timestamp(50), &source).await.unwrap(),
            vec![0, 0]
        );
        assert_eq!(
            resolve(&BranchAt::Timestamp(10_000), &source)
                .await
                .unwrap(),
            vec![3, 1]
        );

        let empty = vec![Arc::new(MemLog::default()) as Arc<dyn PartitionLog>];
        assert_eq!(resolve(&BranchAt::Head, &empty).await.unwrap(), vec![0]);
        assert_eq!(
            resolve(&BranchAt::Timestamp(0), &empty).await.unwrap(),
            vec![0]
        );
    }

    #[tokio::test]
    async fn rejects_offsets_the_source_does_not_have() {
        let source = vec![log_with(&[1, 2]).await, log_with(&[1]).await];
        for (at, needle) in [
            (offsets(&[(0, 1)]), "no offset for partition 1"),
            (
                offsets(&[(0, 1), (1, 0), (2, 0)]),
                "partition 2 does not exist",
            ),
            (offsets(&[(0, 3), (1, 0)]), "cannot branch at offset 3"),
        ] {
            match resolve(&at, &source).await {
                Err(ResolveError::Invalid(msg)) => assert!(msg.contains(needle), "{msg}"),
                other => panic!("{needle}: got {other:?}"),
            }
        }
    }

    fn go(group: &str, partition: i32, offset: Offset) -> GroupOffset {
        GroupOffset {
            group: group.into(),
            partition,
            offset,
        }
    }

    #[tokio::test]
    async fn resolves_group_offsets_within_the_fork_points() {
        let source = vec![log_with(&[1, 2, 3]).await, log_with(&[1, 2]).await];
        source[0].commit_offset("billing", 3).await.unwrap();
        source[1].commit_offset("billing", 1).await.unwrap();
        source[1].commit_offset("audit", 2).await.unwrap();
        // fork points: partition 0 shares 2 records, partition 1 shares 2
        let at = [2, 2];

        let none = resolve_groups(&BranchGroups::None, &source, &at).await;
        assert_eq!(none.unwrap(), vec![]);
        // billing had read past partition 0's fork point: caught up on the fork
        assert_eq!(
            resolve_groups(&BranchGroups::All, &source, &at)
                .await
                .unwrap(),
            vec![go("audit", 1, 2), go("billing", 0, 2), go("billing", 1, 1)]
        );
        let named = BranchGroups::Named(vec!["billing".into()]);
        assert_eq!(
            resolve_groups(&named, &source, &at).await.unwrap(),
            vec![go("billing", 0, 2), go("billing", 1, 1)]
        );
        let unknown = BranchGroups::Named(vec!["billing".into(), "ghost".into()]);
        match resolve_groups(&unknown, &source, &at).await {
            Err(ResolveError::Invalid(msg)) => assert!(msg.contains("ghost"), "{msg}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_faulted_partition_cannot_be_forked() {
        use crate::git::store::GitStore;
        use crate::log::git::GitLog;
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(GitStore::open_or_init(&dir.path().join("d.git")).unwrap());
        let log = GitLog::open_or_create(s.clone(), "orders", 0)
            .await
            .unwrap();
        log.append("app", vec![Record::text(1, "a")]).await.unwrap();
        // someone moves the branch back behind kommit's back; the next append faults it
        let head = s.ref_target("refs/heads/orders/0").unwrap().unwrap();
        let root = s.first_parent_chain(head).unwrap()[0];
        s.cas_ref("refs/heads/orders/0", Some(head), root, "vandal")
            .unwrap();
        assert!(log.append("app", vec![Record::text(2, "b")]).await.is_err());

        let source = vec![Arc::new(log) as Arc<dyn PartitionLog>];
        for at in [BranchAt::Head, offsets(&[(0, 0)]), BranchAt::Timestamp(0)] {
            match resolve(&at, &source).await {
                Err(ResolveError::Log(LogError::Faulted(msg))) => {
                    assert!(msg.contains("partition 0"), "{msg}")
                }
                other => panic!("{at:?}: got {other:?}"),
            }
        }
    }
}
