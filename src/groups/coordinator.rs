//! The classic Kafka group protocol, in memory:
//! `Empty -> PreparingRebalance -> CompletingRebalance -> Stable`.
//!
//! A join parks until every current member has (re)joined, or the rebalance deadline
//! passes; the leader then gets everyone's metadata and hands out assignments in its
//! sync, which releases the followers' parked syncs. A ticker expires silent members
//! and completes overdue rebalances. Assignment itself stays client-side, as in Kafka.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use tokio::sync::oneshot;
use tokio::time::Instant;

/// How often the ticker checks session timeouts and rebalance deadlines.
const TICK: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupState {
    /// The state names Kafka's tools print.
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CoordError {
    #[error("unknown member id")]
    UnknownMemberId,
    #[error("illegal generation")]
    IllegalGeneration,
    #[error("rebalance in progress")]
    RebalanceInProgress,
    #[error("inconsistent group protocol")]
    InconsistentGroupProtocol,
}

impl CoordError {
    pub fn code(&self) -> i16 {
        match self {
            CoordError::UnknownMemberId => ResponseError::UnknownMemberId.code(),
            CoordError::IllegalGeneration => ResponseError::IllegalGeneration.code(),
            CoordError::RebalanceInProgress => ResponseError::RebalanceInProgress.code(),
            CoordError::InconsistentGroupProtocol => {
                ResponseError::InconsistentGroupProtocol.code()
            }
        }
    }
}

pub struct JoinRequest {
    pub group: String,
    /// Empty on a member's first join; the coordinator assigns one.
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub protocol_type: String,
    /// (protocol name, member metadata), in the member's order of preference.
    pub protocols: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinResponse {
    pub generation: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    /// Every member's metadata for the chosen protocol; only the leader gets it.
    pub members: Vec<(String, Bytes)>,
}

pub struct MemberView {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

pub struct GroupView {
    pub state: GroupState,
    pub protocol_type: String,
    pub protocol_name: String,
    pub members: Vec<MemberView>,
}

type JoinReply = oneshot::Sender<Result<JoinResponse, CoordError>>;
type SyncReply = oneshot::Sender<Result<Bytes, CoordError>>;

struct Member {
    client_id: String,
    client_host: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(String, Bytes)>,
    last_seen: Instant,
    assignment: Bytes,
    /// Order of first join, for picking a leader deterministically.
    join_order: u64,
    pending_join: Option<JoinReply>,
    pending_sync: Option<SyncReply>,
}

impl Member {
    fn metadata_for(&self, protocol: &str) -> Bytes {
        self.protocols
            .iter()
            .find(|(name, _)| name == protocol)
            .map(|(_, meta)| meta.clone())
            .unwrap_or_default()
    }

    fn supports(&self, protocol: &str) -> bool {
        self.protocols.iter().any(|(name, _)| name == protocol)
    }
}

struct Group {
    state: GroupState,
    generation: i32,
    protocol_type: String,
    protocol_name: String,
    leader: Option<String>,
    members: BTreeMap<String, Member>,
    next_join_order: u64,
    /// When the current rebalance completes even if some members never rejoin.
    rebalance_deadline: Option<Instant>,
    /// A group coming out of Empty waits this long for its peers to show up.
    initial_delay_until: Option<Instant>,
}

impl Group {
    fn new() -> Self {
        Group {
            state: GroupState::Empty,
            generation: 0,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            members: BTreeMap::new(),
            next_join_order: 0,
            rebalance_deadline: None,
            initial_delay_until: None,
        }
    }

    fn member(&mut self, member_id: &str, generation: i32) -> Result<&mut Member, CoordError> {
        let current = self.generation;
        let member = self
            .members
            .get_mut(member_id)
            .ok_or(CoordError::UnknownMemberId)?;
        if generation != current {
            return Err(CoordError::IllegalGeneration);
        }
        Ok(member)
    }

    /// Moves to PreparingRebalance; members learn about it from heartbeats and rejoin.
    fn start_rebalance(&mut self, now: Instant) {
        if self.state == GroupState::PreparingRebalance {
            return;
        }
        let longest = self
            .members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or_default();
        self.state = GroupState::PreparingRebalance;
        self.rebalance_deadline = Some(now + longest);
        for member in self.members.values_mut() {
            if let Some(reply) = member.pending_sync.take() {
                let _ = reply.send(Err(CoordError::RebalanceInProgress));
            }
        }
    }

    fn remove_member(&mut self, member_id: &str, now: Instant) {
        if let Some(mut member) = self.members.remove(member_id) {
            if let Some(reply) = member.pending_join.take() {
                let _ = reply.send(Err(CoordError::UnknownMemberId));
            }
            if let Some(reply) = member.pending_sync.take() {
                let _ = reply.send(Err(CoordError::UnknownMemberId));
            }
        }
        if self.leader.as_deref() == Some(member_id) {
            self.leader = None;
        }
        if self.members.is_empty() {
            self.become_empty();
        } else if self.state == GroupState::PreparingRebalance {
            self.try_complete_join(now);
        } else {
            self.start_rebalance(now);
        }
    }

    fn become_empty(&mut self) {
        self.state = GroupState::Empty;
        self.leader = None;
        self.protocol_name.clear();
        self.rebalance_deadline = None;
        self.initial_delay_until = None;
    }

    fn try_complete_join(&mut self, now: Instant) {
        if self.state != GroupState::PreparingRebalance {
            return;
        }
        let all_joined = self.members.values().all(|m| m.pending_join.is_some());
        let delay_over = self.initial_delay_until.is_none_or(|t| now >= t);
        let overdue = self.rebalance_deadline.is_some_and(|t| now >= t);
        if (all_joined && delay_over) || overdue {
            self.complete_join(now);
        }
    }

    fn complete_join(&mut self, now: Instant) {
        // Members that did not rejoin in time are out.
        self.members.retain(|_, m| m.pending_join.is_some());
        self.rebalance_deadline = None;
        self.initial_delay_until = None;
        self.generation += 1;
        if self.members.is_empty() {
            self.become_empty();
            return;
        }
        let leader = self
            .leader
            .clone()
            .filter(|l| self.members.contains_key(l))
            .unwrap_or_else(|| {
                self.members
                    .iter()
                    .min_by_key(|(_, m)| m.join_order)
                    .map(|(id, _)| id.clone())
                    .expect("members is not empty")
            });
        let leader_protocols = &self.members[&leader].protocols;
        self.protocol_name = leader_protocols
            .iter()
            .map(|(name, _)| name)
            .find(|name| self.members.values().all(|m| m.supports(name)))
            .unwrap_or(&leader_protocols[0].0)
            .clone();
        self.leader = Some(leader.clone());
        self.state = GroupState::CompletingRebalance;
        let everyone: Vec<(String, Bytes)> = self
            .members
            .iter()
            .map(|(id, m)| (id.clone(), m.metadata_for(&self.protocol_name)))
            .collect();
        for (id, member) in self.members.iter_mut() {
            member.assignment = Bytes::new();
            // A parked join sends no heartbeats; the session restarts now, as in Kafka.
            member.last_seen = now;
            if let Some(reply) = member.pending_join.take() {
                let _ = reply.send(Ok(JoinResponse {
                    generation: self.generation,
                    protocol_type: self.protocol_type.clone(),
                    protocol_name: self.protocol_name.clone(),
                    leader: leader.clone(),
                    member_id: id.clone(),
                    members: if *id == leader {
                        everyone.clone()
                    } else {
                        Vec::new()
                    },
                }));
            }
        }
    }

    fn tick(&mut self, now: Instant) {
        // A member parked in a join or sync is alive; it is waiting on us, not silent.
        let expired: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| {
                m.pending_join.is_none()
                    && m.pending_sync.is_none()
                    && now - m.last_seen > m.session_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            tracing::info!(member = %id, "consumer group member session expired");
            self.remove_member(&id, now);
        }
        self.try_complete_join(now);
    }
}

pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
    initial_delay: Duration,
}

impl Coordinator {
    /// Creates the coordinator and its ticker. `known` groups (those with committed
    /// offsets) start out Empty so they can be listed and described.
    pub fn start(initial_delay: Duration, known: Vec<String>) -> Arc<Self> {
        let groups = known.into_iter().map(|g| (g, Group::new())).collect();
        let coordinator = Arc::new(Coordinator {
            groups: Mutex::new(groups),
            initial_delay,
        });
        let weak: Weak<Coordinator> = Arc::downgrade(&coordinator);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            loop {
                interval.tick().await;
                let Some(coordinator) = weak.upgrade() else {
                    return;
                };
                coordinator.tick(Instant::now());
            }
        });
        coordinator
    }

    fn tick(&self, now: Instant) {
        for group in self.groups.lock().unwrap().values_mut() {
            group.tick(now);
        }
    }

    pub async fn join(&self, req: JoinRequest) -> Result<JoinResponse, CoordError> {
        let reply = {
            let now = Instant::now();
            let mut groups = self.groups.lock().unwrap();
            let group = groups.entry(req.group.clone()).or_insert_with(Group::new);
            if req.protocols.is_empty()
                || (!group.members.is_empty() && group.protocol_type != req.protocol_type)
            {
                return Err(CoordError::InconsistentGroupProtocol);
            }
            let others_share_a_protocol = req.protocols.iter().any(|(name, _)| {
                group
                    .members
                    .iter()
                    .filter(|(id, _)| **id != req.member_id)
                    .all(|(_, m)| m.supports(name))
            });
            if !others_share_a_protocol {
                return Err(CoordError::InconsistentGroupProtocol);
            }
            let member_id = if req.member_id.is_empty() {
                format!("{}-{}", req.client_id, uuid::Uuid::new_v4())
            } else if group.members.contains_key(&req.member_id) {
                req.member_id.clone()
            } else {
                return Err(CoordError::UnknownMemberId);
            };
            let (tx, rx) = oneshot::channel();
            let join_order = group.next_join_order;
            let member = group.members.entry(member_id).or_insert_with(|| Member {
                client_id: req.client_id.clone(),
                client_host: req.client_host.clone(),
                session_timeout: req.session_timeout,
                rebalance_timeout: req.rebalance_timeout,
                protocols: Vec::new(),
                last_seen: now,
                assignment: Bytes::new(),
                join_order,
                pending_join: None,
                pending_sync: None,
            });
            if member.join_order == join_order {
                group.next_join_order += 1;
            }
            member.session_timeout = req.session_timeout;
            member.rebalance_timeout = req.rebalance_timeout;
            member.protocols = req.protocols;
            member.last_seen = now;
            // A newer join from the same member replaces (and fails) the older one.
            if let Some(old) = member.pending_join.replace(tx) {
                let _ = old.send(Err(CoordError::UnknownMemberId));
            }
            group.protocol_type = req.protocol_type;
            match group.state {
                GroupState::Empty => {
                    group.start_rebalance(now);
                    group.initial_delay_until = Some(now + self.initial_delay);
                    let deadline = group.rebalance_deadline.unwrap_or(now);
                    group.rebalance_deadline = Some(deadline.max(now + self.initial_delay));
                }
                GroupState::PreparingRebalance => {}
                GroupState::CompletingRebalance | GroupState::Stable => group.start_rebalance(now),
            }
            group.try_complete_join(now);
            rx
        };
        reply.await.unwrap_or(Err(CoordError::UnknownMemberId))
    }

    pub async fn sync(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
        assignments: Vec<(String, Bytes)>,
    ) -> Result<Bytes, CoordError> {
        let reply = {
            let mut groups = self.groups.lock().unwrap();
            let group = groups
                .get_mut(group_id)
                .ok_or(CoordError::UnknownMemberId)?;
            let state = group.state;
            let is_leader = group.leader.as_deref() == Some(member_id);
            let member = group.member(member_id, generation)?;
            member.last_seen = Instant::now();
            match state {
                GroupState::Empty => return Err(CoordError::UnknownMemberId),
                GroupState::PreparingRebalance => return Err(CoordError::RebalanceInProgress),
                GroupState::Stable => return Ok(member.assignment.clone()),
                GroupState::CompletingRebalance if !is_leader => {
                    let (tx, rx) = oneshot::channel();
                    member.pending_sync = Some(tx);
                    rx
                }
                GroupState::CompletingRebalance => {
                    let mut assignments: HashMap<String, Bytes> = assignments.into_iter().collect();
                    for (id, member) in group.members.iter_mut() {
                        member.assignment = assignments.remove(id).unwrap_or_default();
                        if let Some(reply) = member.pending_sync.take() {
                            let _ = reply.send(Ok(member.assignment.clone()));
                        }
                    }
                    group.state = GroupState::Stable;
                    return Ok(group.members[member_id].assignment.clone());
                }
            }
        };
        reply.await.unwrap_or(Err(CoordError::RebalanceInProgress))
    }

    pub fn heartbeat(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
    ) -> Result<(), CoordError> {
        let mut groups = self.groups.lock().unwrap();
        let group = groups
            .get_mut(group_id)
            .ok_or(CoordError::UnknownMemberId)?;
        let state = group.state;
        let member = group.member(member_id, generation)?;
        member.last_seen = Instant::now();
        if state == GroupState::PreparingRebalance {
            return Err(CoordError::RebalanceInProgress);
        }
        Ok(())
    }

    pub fn leave(
        &self,
        group_id: &str,
        member_ids: &[String],
    ) -> Vec<(String, Result<(), CoordError>)> {
        let now = Instant::now();
        let mut groups = self.groups.lock().unwrap();
        let Some(group) = groups.get_mut(group_id) else {
            return member_ids
                .iter()
                .map(|id| (id.clone(), Err(CoordError::UnknownMemberId)))
                .collect();
        };
        member_ids
            .iter()
            .map(|id| {
                if group.members.contains_key(id) {
                    group.remove_member(id, now);
                    (id.clone(), Ok(()))
                } else {
                    (id.clone(), Err(CoordError::UnknownMemberId))
                }
            })
            .collect()
    }

    /// Whether an offset commit from this member and generation may proceed.
    /// Generation -1 is a group-less commit (simple consumers) and is always allowed.
    pub fn validate_commit(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
    ) -> Result<(), CoordError> {
        if generation < 0 {
            return Ok(());
        }
        let mut groups = self.groups.lock().unwrap();
        let group = groups
            .get_mut(group_id)
            .ok_or(CoordError::UnknownMemberId)?;
        let state = group.state;
        group.member(member_id, generation)?;
        // As in Kafka: members commit while a rebalance is being prepared (that is what
        // it is for), but not once it is completing, when partitions may have moved.
        if state == GroupState::CompletingRebalance {
            return Err(CoordError::RebalanceInProgress);
        }
        Ok(())
    }

    /// Records that a group exists (it has committed offsets) without any members.
    pub fn ensure_group(&self, group_id: &str) {
        self.groups
            .lock()
            .unwrap()
            .entry(group_id.to_string())
            .or_insert_with(Group::new);
    }

    /// Every known group with its state and protocol type, sorted by name.
    pub fn list(&self) -> Vec<(String, GroupState, String)> {
        let groups = self.groups.lock().unwrap();
        let mut out: Vec<_> = groups
            .iter()
            .map(|(id, g)| (id.clone(), g.state, g.protocol_type.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn describe(&self, group_id: &str) -> Option<GroupView> {
        let groups = self.groups.lock().unwrap();
        let group = groups.get(group_id)?;
        Some(GroupView {
            state: group.state,
            protocol_type: group.protocol_type.clone(),
            protocol_name: group.protocol_name.clone(),
            members: group
                .members
                .iter()
                .map(|(id, m)| MemberView {
                    member_id: id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    metadata: m.metadata_for(&group.protocol_name),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELAY: Duration = Duration::from_millis(300);

    fn join_req(member_id: &str, client: &str) -> JoinRequest {
        JoinRequest {
            group: "g".into(),
            member_id: member_id.into(),
            client_id: client.into(),
            client_host: "/127.0.0.1".into(),
            session_timeout: Duration::from_secs(1),
            rebalance_timeout: Duration::from_secs(5),
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), Bytes::from(format!("meta-{client}")))],
        }
    }

    /// Two members joined and synced into a Stable generation 1. Returns (leader, follower).
    async fn stable_pair(c: &Arc<Coordinator>) -> (String, String) {
        let (a, b) = tokio::join!(c.join(join_req("", "a")), c.join(join_req("", "b")));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.generation, 1);
        assert_eq!(b.generation, 1);
        let (leader, follower) = if a.leader == a.member_id {
            (a, b)
        } else {
            (b, a)
        };
        let assignments = vec![
            (leader.member_id.clone(), Bytes::from_static(b"L")),
            (follower.member_id.clone(), Bytes::from_static(b"F")),
        ];
        let (fs, ls) = tokio::join!(c.sync("g", 1, &follower.member_id, vec![]), async {
            tokio::task::yield_now().await;
            c.sync("g", 1, &leader.member_id, assignments.clone()).await
        });
        assert_eq!(fs.unwrap(), "F");
        assert_eq!(ls.unwrap(), "L");
        (leader.member_id, follower.member_id)
    }

    #[tokio::test(start_paused = true)]
    async fn a_lone_member_leads_generation_one() {
        let c = Coordinator::start(DELAY, vec![]);
        let r = c.join(join_req("", "solo")).await.unwrap();
        assert_eq!(r.generation, 1);
        assert_eq!(r.leader, r.member_id);
        assert!(r.member_id.starts_with("solo-"));
        assert_eq!(r.protocol_name, "range");
        assert_eq!(
            r.members,
            vec![(r.member_id.clone(), Bytes::from("meta-solo"))]
        );
        let mine = c
            .sync(
                "g",
                1,
                &r.member_id,
                vec![(r.member_id.clone(), Bytes::from("A"))],
            )
            .await
            .unwrap();
        assert_eq!(mine, "A");
        c.heartbeat("g", 1, &r.member_id).unwrap();
        assert_eq!(c.describe("g").unwrap().state, GroupState::Stable);
    }

    #[tokio::test(start_paused = true)]
    async fn members_starting_together_share_one_generation() {
        let c = Coordinator::start(DELAY, vec![]);
        let (a, b) = tokio::join!(c.join(join_req("", "a")), c.join(join_req("", "b")));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!((a.generation, b.generation), (1, 1));
        assert_eq!(a.leader, b.leader);
        let (leader, follower) = if a.leader == a.member_id {
            (&a, &b)
        } else {
            (&b, &a)
        };
        assert_eq!(leader.members.len(), 2);
        assert!(follower.members.is_empty());
        stable_pair_after(&c, leader, follower).await;
    }

    async fn stable_pair_after(
        c: &Arc<Coordinator>,
        leader: &JoinResponse,
        follower: &JoinResponse,
    ) {
        let assignments = vec![
            (leader.member_id.clone(), Bytes::from_static(b"L")),
            (follower.member_id.clone(), Bytes::from_static(b"F")),
        ];
        let (fs, ls) = tokio::join!(c.sync("g", 1, &follower.member_id, vec![]), async {
            tokio::task::yield_now().await;
            c.sync("g", 1, &leader.member_id, assignments.clone()).await
        });
        assert_eq!(fs.unwrap(), "F");
        assert_eq!(ls.unwrap(), "L");
    }

    #[tokio::test(start_paused = true)]
    async fn a_newcomer_triggers_a_rebalance_everyone_rejoins() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, follower) = stable_pair(&c).await;
        let newcomer = tokio::spawn({
            let c = c.clone();
            async move { c.join(join_req("", "c")).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            c.heartbeat("g", 1, &leader),
            Err(CoordError::RebalanceInProgress)
        );
        let (l, f) = tokio::join!(
            c.join(join_req(&leader, "a")),
            c.join(join_req(&follower, "b"))
        );
        let n = newcomer.await.unwrap().unwrap();
        assert_eq!(
            (l.unwrap().generation, f.unwrap().generation, n.generation),
            (2, 2, 2)
        );
        assert_eq!(c.describe("g").unwrap().members.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_member_is_evicted_and_the_survivor_takes_over() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, follower) = stable_pair(&c).await;
        // only the follower keeps heartbeating; the leader goes silent
        let mut saw_rebalance = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if c.heartbeat("g", 1, &follower) == Err(CoordError::RebalanceInProgress) {
                saw_rebalance = true;
                break;
            }
        }
        assert!(saw_rebalance, "silent member was never evicted");
        let r = c.join(join_req(&follower, "b")).await.unwrap();
        assert_eq!(r.generation, 2);
        assert_eq!(r.leader, follower);
        assert_eq!(r.members.len(), 1);
        assert_eq!(
            c.heartbeat("g", 2, &leader),
            Err(CoordError::UnknownMemberId)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn early_rejoiners_survive_a_rebalance_slower_than_their_session() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, follower) = stable_pair(&c).await;
        let newcomer = tokio::spawn({
            let c = c.clone();
            async move { c.join(join_req("", "c")).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        // the leader rejoins at once; the follower keeps heartbeating (as Java's
        // heartbeat thread does) but only rejoins after 3s; the session timeout is 1s
        let early = tokio::spawn({
            let c = c.clone();
            let leader = leader.clone();
            async move { c.join(join_req(&leader, "a")).await }
        });
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(
                c.heartbeat("g", 1, &follower),
                Err(CoordError::RebalanceInProgress)
            );
        }
        let late = c.join(join_req(&follower, "b")).await.unwrap();
        let early = early.await.unwrap().unwrap();
        newcomer.await.unwrap().unwrap();
        assert_eq!(early.generation, late.generation);
        // a few ticks later, the early rejoiner must still be a member
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(c.heartbeat("g", early.generation, &leader), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn a_follower_waiting_on_a_slow_leaders_sync_is_not_evicted() {
        let c = Coordinator::start(DELAY, vec![]);
        let (a, b) = tokio::join!(c.join(join_req("", "a")), c.join(join_req("", "b")));
        let (a, b) = (a.unwrap(), b.unwrap());
        let (leader, follower) = if a.leader == a.member_id {
            (a, b)
        } else {
            (b, a)
        };
        let parked = tokio::spawn({
            let c = c.clone();
            let id = follower.member_id.clone();
            async move { c.sync("g", 1, &id, vec![]).await }
        });
        // the leader computes assignments for longer than the follower's session timeout,
        // heartbeating meanwhile as a real client's background thread does
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(c.heartbeat("g", 1, &leader.member_id), Ok(()));
        }
        c.sync(
            "g",
            1,
            &leader.member_id,
            vec![(follower.member_id.clone(), Bytes::from_static(b"F"))],
        )
        .await
        .unwrap();
        assert_eq!(parked.await.unwrap().unwrap(), "F");
    }

    #[tokio::test(start_paused = true)]
    async fn commits_are_allowed_while_preparing_and_refused_while_completing() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, follower) = stable_pair(&c).await;
        // a newcomer starts a rebalance: members commit before giving up partitions
        let newcomer = tokio::spawn({
            let c = c.clone();
            async move { c.join(join_req("", "c")).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(c.validate_commit("g", 1, &leader), Ok(()));
        // once the join completes, generation 1 commits are stale and the group is
        // completing: partitions may already belong to someone else
        let (l, f) = tokio::join!(
            c.join(join_req(&leader, "a")),
            c.join(join_req(&follower, "b"))
        );
        newcomer.await.unwrap().unwrap();
        let generation = l.unwrap().generation;
        f.unwrap();
        assert_eq!(
            c.validate_commit("g", generation, &leader),
            Err(CoordError::RebalanceInProgress)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn when_the_leader_leaves_the_follower_leads() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, follower) = stable_pair(&c).await;
        assert_eq!(
            c.leave("g", std::slice::from_ref(&leader)),
            vec![(leader.clone(), Ok(()))]
        );
        assert_eq!(
            c.heartbeat("g", 1, &follower),
            Err(CoordError::RebalanceInProgress)
        );
        let r = c.join(join_req(&follower, "b")).await.unwrap();
        assert_eq!((r.generation, r.leader.as_str()), (2, follower.as_str()));
        // last member leaving empties the group
        c.leave("g", &[follower]);
        assert_eq!(c.describe("g").unwrap().state, GroupState::Empty);
    }

    #[tokio::test(start_paused = true)]
    async fn bad_requests_get_kafka_errors() {
        let c = Coordinator::start(DELAY, vec![]);
        let (leader, _) = stable_pair(&c).await;
        assert_eq!(
            c.sync("g", 7, &leader, vec![]).await,
            Err(CoordError::IllegalGeneration)
        );
        assert_eq!(
            c.heartbeat("g", 1, "ghost"),
            Err(CoordError::UnknownMemberId)
        );
        assert_eq!(
            c.heartbeat("nope", 1, &leader),
            Err(CoordError::UnknownMemberId)
        );
        assert_eq!(
            c.join(join_req("ghost", "x")).await,
            Err(CoordError::UnknownMemberId)
        );
        let mut other_type = join_req("", "x");
        other_type.protocol_type = "connect".into();
        assert_eq!(
            c.join(other_type).await,
            Err(CoordError::InconsistentGroupProtocol)
        );
        let mut no_overlap = join_req("", "x");
        no_overlap.protocols = vec![("sticky".into(), Bytes::new())];
        assert_eq!(
            c.join(no_overlap).await,
            Err(CoordError::InconsistentGroupProtocol)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn commits_are_checked_against_live_membership() {
        let c = Coordinator::start(DELAY, vec!["old".into()]);
        let (leader, _) = stable_pair(&c).await;
        assert_eq!(c.validate_commit("g", 1, &leader), Ok(()));
        assert_eq!(c.validate_commit("g", -1, ""), Ok(()));
        assert_eq!(
            c.validate_commit("g", 2, &leader),
            Err(CoordError::IllegalGeneration)
        );
        assert_eq!(
            c.validate_commit("g", 1, "ghost"),
            Err(CoordError::UnknownMemberId)
        );
        // groups known only from committed offsets are listed as Empty
        let listed: Vec<_> = c.list().into_iter().map(|(g, s, _)| (g, s)).collect();
        assert!(listed.contains(&("old".to_string(), GroupState::Empty)));
        assert!(listed.contains(&("g".to_string(), GroupState::Stable)));
    }
}
