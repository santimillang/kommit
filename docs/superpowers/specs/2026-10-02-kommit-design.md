# kommit — design

**Date:** 2026-10-02
**Status:** approved in conversation, pending written-spec review

## 1. What this is

kommit is a single-broker, Kafka-wire-compatible message broker written in pure Rust whose
only storage is a Git repository. **A Kafka record is a Git commit, and a partition is a
branch.** It is a for-fun project: the goal is that real Kafka clients work against it
unmodified, and that the resulting repository is a faithful, browsable Git history of every
topic.

### Goals

- Real clients connect without knowing: `kcat`, librdkafka (via the `rdkafka` crate), and the
  Java client (via Kafka's own CLI tools).
- The Git repository is the single source of truth. A restart rebuilds all broker state from
  refs and commits; there are no side files that can drift.
- The joke is literally true and tested: `git log orders/0` shows the records,
  `git rev-list --count` equals consumer lag, `git fsck` is clean, `git push --all` publishes
  the cluster to GitHub.

### Non-goals

Multi-broker clustering and replication, transactions, the KIP-848 consumer protocol,
retention/deletion, TLS/SASL/ACLs (plaintext only), durability guarantees (no fsync), and
performance. Thousands of records per second is fine.

## 2. Technology

- Rust, `tokio` for networking.
- `kafka-protocol` crate for request/response types and `RecordBatch` encode/decode, with its
  gzip/snappy/lz4/zstd features.
- `gix` (gitoxide) for all Git access. **Pure Rust: no libgit2, no shelling out to `git`** in
  the broker. (Tests may shell out to the real `git` CLI to verify the repository.)
- Risk: `gix`'s write path (object writes, ref transactions with expected-previous-value) is
  less battle-tested than libgit2's. M1 exercises it first, so problems surface early.

## 3. Architecture

One binary, one bare Git repository as the data directory. Five modules, each with one job:

| Module   | Responsibility | Depends on |
|----------|----------------|------------|
| `net`    | TCP listener, length-prefixed framing, request-header decode, dispatch by API key/version | `api` |
| `api`    | One handler per Kafka API; builds responses and per-partition error codes | `log` and `groups` traits, topic catalog from `git` meta log |
| `log`    | `PartitionLog` trait plus `MemLog` (tests) and `GitLog` (real) | `git` |
| `git`    | Record↔commit encoding, ref layout, meta log, offset index | `gix` |
| `groups` | Consumer-group coordinator state machine and committed offsets (M2) | `log`/`git` for offset refs |

```rust
trait PartitionLog {
    async fn append(&self, records: Vec<Record>) -> Result<Offset>; // returns base offset
    async fn read(&self, from: Offset, max_bytes: usize) -> Result<Vec<Record>>;
    fn high_watermark(&self) -> Offset;
    fn log_start(&self) -> Offset; // always 0: no retention
}
```

Handlers never touch Git directly, so the whole protocol layer is testable against `MemLog`.
Each partition has **one writer task**; appends to a partition are serialized through it.

## 4. Storage layout

### 4.1 Refs

| Ref | Meaning |
|-----|---------|
| `refs/heads/<topic>/<partition>` | A partition. Real branches, so `git branch` lists partitions and GitHub's UI shows them. Kafka topic names cannot contain `/`, so names never collide. |
| `refs/groups/<group>/<topic>/<partition>` | A consumer group's committed position (§6.1). Not a branch. |
| `refs/kommit/meta` | The metadata log (§4.4). |

### 4.2 A record is a commit

| Kafka record | Git commit |
|---|---|
| value | commit message; if not valid UTF-8 (or contains NUL), base64-encoded and marked with the extra header `kommit-value base64` |
| key | extra commit header `kommit-key <field>`; absent when the key is null |
| headers | repeated extra commit headers `kommit-header <name-field> <value-field\|null>`, in order |
| null value (tombstone) | extra header `kommit-value null`, empty message |
| timestamp | extra header `kommit-ts <ms>`; committer time is the same instant in seconds, clamped to 0..year 9999 |
| producer | author `<client.id> <client.id@kommit>`, client id sanitized to `[A-Za-z0-9._-]` (empty: `anonymous`) |
| tree | the empty tree, shared by every commit |

A `<field>` is `t:<text>` when the bytes are non-empty printable ASCII without spaces,
otherwise `b:<base64>` (so empty values survive Git's header format). Record header
names collapse duplicates, because `kafka-protocol` decodes headers into a map.

These spellings were fixed in M1 and are pinned by tests; the encoding must keep the
repository `git fsck`-clean, which is asserted in CI.

### 4.3 Offsets and the sentinel root

Every partition branch starts with a **sentinel root commit** (empty tree, message
`kommit: partition <topic>/<partition> created`). Record offset *k* is the commit at
first-parent position *k+1*. The sentinel lets an empty partition exist as a branch and gives
committed offset 0 something to point at.

Git cannot jump to "the Nth commit", so `GitLog` keeps an **in-memory offset index**
(`Vec<ObjectId>`, position → commit), built at startup by walking each branch's first-parent
chain and extended on every append. Startup cost grows with topic size; that is accepted
because it keeps Git the only source of truth.

### 4.4 Metadata log

`refs/kommit/meta` is a chain of commits, one per cluster event (the KRaft
`__cluster_metadata` idea, in Git). Event kinds: `CreateTopic`, `BranchTopic` (M3),
`AllocateProducerIds` (M2). Each event is a commit whose message is the event serialized as
TOML. Startup replays the chain. The meta log is authoritative: a branch created by hand
with `git branch` while the broker runs is not a topic.

## 5. Data flow (M1)

### 5.1 Produce

1. `net` decodes `ProduceRequest`. `acks=0` sends no response.
2. Per partition, record batches are CRC-checked, decompressed, and decoded into `Record`s,
   which go to that partition's writer task.
3. The writer writes one commit per record, each parented on the previous, starting from the
   current head.
4. It moves `refs/heads/<topic>/<partition>` with **one** ref transaction whose expected
   previous value is the old head. **The ref move is the commit point:** a crash before it
   leaves unreachable objects for `git gc`, mirroring Kafka's high watermark. A batch of N
   records becomes visible atomically.
5. The writer extends the offset index, notifies waiting fetches, and returns the base offset.

### 5.2 Fetch

1. Resolve the requested offset through the index; unknown → `OFFSET_OUT_OF_RANGE`.
2. Read commits forward until `max_bytes`, decode them to records, encode a single
   uncompressed v2 `RecordBatch` with the correct base offset. Batches are not byte-identical
   to what was produced; clients do not care.
3. **Long poll:** if no data, wait on the partition's high-watermark notification up to
   `max_wait_ms`.

### 5.3 ListOffsets

Earliest is always 0 (no retention); latest is the high watermark. Lookup by timestamp
returns the first offset whose `kommit-timestamp-ms` is ≥ the target, found by a linear scan
of the partition. Producer timestamps are not guaranteed monotonic, so no binary search.

### 5.4 Topics

Created by `CreateTopics`, or auto-created by a `Metadata` request that allows it
(configurable, default on so `kcat` works out of the box). `Produce` never auto-creates:
clients send `Metadata` first, as with Kafka. Partition counts are capped at 1000, and
creation refuses branches that already exist; both are checked before the `CreateTopic`
meta event is appended, then one sentinel-rooted branch is created per partition.

### 5.5 Startup

1. Take the repository lock file; refuse to start if another broker holds it.
2. Replay `refs/kommit/meta`.
3. For each partition, walk the first-parent chain to the sentinel and build the offset index.
4. Load committed offsets from `refs/groups/*`.

## 6. Consumer groups and producers (M2)

### 6.1 Committed offsets

A committed offset *c* ("next record to read") is stored as `refs/groups/<g>/<t>/<p>`
pointing at first-parent position *c* of the partition (the sentinel for *c = 0*, record
*c−1* otherwise). Therefore `git rev-list --count refs/groups/g/t/p..t/p` equals the lag.
Offset-commit metadata strings are not stored (returned empty). (An earlier draft promised a
reflog audit trail per commit; Git does not keep reflogs for custom ref namespaces like
`refs/groups/` in a bare repo without extra config, so M2 does not provide one. See §6.4.)

### 6.2 Group coordination

Classic group protocol only. `FindCoordinator` always returns this broker. In-memory state
machine `Empty → PreparingRebalance → CompletingRebalance → Stable`: `JoinGroup` collects
members until the rebalance timeout, the leader receives all members' protocol metadata,
`SyncGroup` distributes the leader's assignment, `Heartbeat` returns `REBALANCE_IN_PROGRESS`
during rebalances, and members are expired on session timeout. Partition assignment stays
client-side. Membership is not persisted; clients rejoin after a restart. `ListGroups` and
`DescribeGroups` are implemented so `kafka-consumer-groups.sh` works. Static membership is out
of scope.

### 6.3 Idempotent producers

The Java client enables idempotence by default, so `InitProducerId` is implemented. Producer
ids are allocated from a counter persisted via `AllocateProducerIds` meta events (blocks of
ids, so not every producer writes a meta commit). Retried duplicate batches are dropped using
the last sequence number per (producer id, partition), held in memory. Transactions are out of
scope.

### 6.4 As built in M2

- Group ids that are not valid topic-style names are stored in refs as `%` + hex of their
  UTF-8 bytes (`refs/groups/%6d792067726f7570/...` for `my group`).
- Committing an offset beyond the high watermark returns `OFFSET_OUT_OF_RANGE`: a ref cannot
  point past the branch head.
- JoinGroup assigns member ids on the first join (no `MEMBER_ID_REQUIRED` round trip). A
  group leaving Empty waits `--group-initial-rebalance-delay-ms` (default 3000, as Kafka) so
  members that start together share a generation.
- Producer ids come in blocks of 1000. De-duplication remembers the last 5 batches per
  (producer, topic, partition), returns the original offset for a retried batch, and refuses
  sequence gaps; producers seen first after a restart may start at any sequence.
- InitProducerId is advertised up to v5: kafka-protocol 0.18 cannot decode v6 requests.
- Like Kafka, completing a join restarts every member's session, and members parked in a join
  or sync never expire. Offset commits are allowed during PreparingRebalance (consumers commit
  on revoke) and refused during CompletingRebalance.
- De-duplication tracks the producer epoch (KIP-360): a newer epoch restarts sequences at 0,
  an older one gets `INVALID_PRODUCER_EPOCH`.
- No offset-commit audit trail: the committed-offset refs are not reflogged.
- Real clients are tested in CI: `kcat -G`, and Apache Kafka 4.1's console producer,
  console consumer group and `kafka-consumer-groups --describe`.

## 7. Branching topics (M3)

A fork is requested through the normal `CreateTopics` API, with the configs
`kommit.branch.from` and `kommit.branch.at`, so any Admin client can make one (Java
`Admin#createTopics`, librdkafka). `kafka-topics.sh` cannot (see §7.1). kommit's own CLI:

```
kommit branch --bootstrap localhost:9092 orders orders-replay \
  --at 2026-10-01T12:00:00Z   # or: 0:42,1:17   or: head
```

`at` accepts an RFC 3339 timestamp (resolved per partition like `ListOffsets`), an explicit
per-partition offset list, or `head`. The fork has the source's partition count. kommit
appends a `BranchTopic` meta event and creates `refs/heads/orders-replay/<p>` at the resolved
commit of each source partition. The fork shares history: same commits, same SHAs, same
offsets, zero copy. Writes to the fork diverge it like any Git branch and never affect the
source. `kommit branch <from> <to> --at <spec>` is a thin CLI over the same API.

Stretch, not committed: forking consumer-group offsets along with the topic; adopting
externally created branches; `git merge` for topics.

### 7.1 As built in M3

- `kafka-topics.sh --config kommit.branch.from=…` cannot work: Kafka's `TopicCommand` runs
  `LogConfig.validateNames` and refuses unknown config names before sending anything. The
  CI pins this, so the docs notice if Kafka relaxes it.
- Fork point *n* means the fork shares records `0..n` and starts with high watermark *n*;
  any *n* in `0..=high_watermark` is allowed. A timestamp *T* forks each partition at its
  first record with timestamp `>= T`, or at the head when there is none. `head` is the
  default when `at` is omitted.
- An offset list names every partition exactly once. Numbers with a leading zero do not
  count as offsets, so `12:00` is rejected rather than read as partition 12.
- `num_partitions` must be -1 or the source's count.
- Every problem with the branch configs is `INVALID_CONFIG` with a message saying what is
  wrong, including an unknown source and an unknown `kommit.*` config. Other configs are
  still ignored. A bad or taken topic name keeps its usual code, and a faulted source
  partition is `KAFKA_STORAGE_ERROR`: it cannot be forked until a restart.
- A fork checks that every fork point lies on its root topic's history before anything is
  recorded. If the branches fail to open after the meta commit, the name stays taken
  until a restart, which finishes the fork from the recorded SHAs.
- Forking holds no broker-wide lock: the new name is reserved while history is walked.
- `BranchTopic { name, topic_id, from, root, at, heads }`: `heads` are the fork-point SHAs and
  are authoritative on replay, so a crash between the meta commit and the refs is repaired
  at the right commits. `root` is the topic whose sentinels the branches start at, which
  for a fork of a fork is the original topic.
- Fork points are read from the source's branches in Git, not from memory.
- Consumer-group offsets are not forked: a group on the fork starts from
  `auto.offset.reset`, and its lag is still `git rev-list --count`.
- Real clients are tested in CI: kcat forks at a timestamp, replays the fork in a fresh group
  and diverges it; a Java console consumer group replays a fork made by `kommit branch`.

## 8. Error handling

- `ApiVersions` advertises only implemented APIs and version ranges; an unknown API key
  closes the connection.
- Per-partition errors use real Kafka codes: `UNKNOWN_TOPIC_OR_PARTITION`,
  `OFFSET_OUT_OF_RANGE`, `CORRUPT_MESSAGE` (CRC failure), `INVALID_TOPIC_EXCEPTION`,
  `TOPIC_ALREADY_EXISTS`, `UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION`, `REBALANCE_IN_PROGRESS`.
- A lost compare-and-swap on a partition ref means something else moved the branch. The
  partition is marked faulted and returns `KAFKA_STORAGE_ERROR` until restart, with a loud log
  line. It is never retried silently. Git I/O errors also map to `KAFKA_STORAGE_ERROR`.
- Only first parents are followed, so a `git merge` into a partition cannot corrupt offsets. A
  partition branch that does not reach a sentinel root fails startup with an error naming it.
- A panic in a connection task drops that connection, not the broker.

## 9. Testing

TDD throughout: failing test first, then the minimal implementation.

- **Property tests** (`proptest`): arbitrary keys, values (binary, NUL, empty, null), and
  headers round-trip record → commit → record.
- **Unit tests:** offset index; group state machine with a fake clock.
- **API tests:** handlers against `MemLog` using real encoded requests.
- **`GitLog` tests** on temp repositories, verified with the real `git` CLI: `git log` shows
  the records, `git rev-list --count` equals lag, `git fsck` is clean.
- **Real-client integration:** `kcat` and `rdkafka` from M1; Kafka's Java CLI tools from the
  `apache/kafka` Docker image from M2.
- **CI:** GitHub Actions running `cargo fmt --check`, `cargo clippy -D warnings`, unit and
  integration tests. The push demo targets a local bare remote, not GitHub.

## 10. Milestones

| Milestone | Scope | Demo |
|-----------|-------|------|
| **M0** skeleton | Crate layout, CI, framing, `ApiVersions` | `kcat -L` gets an `ApiVersions` answer |
| **M1** kcat on Git | `Metadata`, `CreateTopics`, `Produce`, `Fetch`, `ListOffsets`, `GitLog`, meta log, startup rebuild | `kcat` produce → restart → consume; `git log orders/0`; `git push --all` to GitHub |
| **M2** consumer groups | Offset commit/fetch, group coordination, `InitProducerId`, `ListGroups`/`DescribeGroups` | Two Java console consumers share a group; `kafka-consumer-groups.sh` lag equals `git rev-list --count` |
| **M3** branching | `CreateTopics` with `kommit.branch.*`, `kommit branch` CLI | `kommit branch` forks `orders` at a timestamp, a fresh consumer replays the fork, the fork diverges |

## 11. Workflow

For-fun repository: commits go straight to `main`. Conventional Commits.
