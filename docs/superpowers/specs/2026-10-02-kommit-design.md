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
| value | commit message; if not valid UTF-8 (or contains NUL), base64-encoded and marked with the extra header `kommit-value-encoding base64` |
| key | extra commit header `kommit-key` (base64 when binary, marked by `kommit-key-encoding base64`); absent when the key is null |
| headers | extra commit headers `kommit-header-<index>` carrying `<base64 name> <base64 value>`, preserving order and duplicates |
| null value (tombstone) | extra header `kommit-value-null`, empty message |
| timestamp | committer time (milliseconds kept in extra header `kommit-timestamp-ms`, since Git stores seconds) |
| producer | author `<client.id> <client.id@kommit>` |
| tree | the empty tree, shared by every commit |

Exact header spellings are fixed in M1 and pinned by tests; the encoding must keep the
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

Created by `CreateTopics`, or auto-created on first `Metadata`/`Produce` (configurable,
default on so `kcat` works out of the box). Creation appends a `CreateTopic` meta event, then
creates one sentinel-rooted branch per partition.

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
Every commit of an offset appends a reflog entry naming the member, so the reflog is an audit
trail. Offset-commit metadata strings are not stored (returned empty).

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

## 7. Branching topics (M3)

A fork is requested through the normal `CreateTopics` API, so any Kafka admin tool works:

```
kafka-topics.sh --create --topic orders-replay \
  --config kommit.branch.from=orders \
  --config kommit.branch.at=2026-10-01T12:00:00Z   # or: 0:42,1:17   or: head
```

`at` accepts an RFC 3339 timestamp (resolved per partition like `ListOffsets`), an explicit
per-partition offset list, or `head`. The fork has the source's partition count. kommit
appends a `BranchTopic` meta event and creates `refs/heads/orders-replay/<p>` at the resolved
commit of each source partition. The fork shares history: same commits, same SHAs, same
offsets, zero copy. Writes to the fork diverge it like any Git branch and never affect the
source. `kommit branch <from> <to> --at <spec>` is a thin CLI over the same API.

Stretch, not committed: forking consumer-group offsets along with the topic; adopting
externally created branches; `git merge` for topics.

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
| **M3** branching | `CreateTopics` with `kommit.branch.*`, `kommit branch` CLI | Fork `orders` at a timestamp, replay the fork with a fresh consumer, diverge it |

## 11. Workflow

For-fun repository: commits go straight to `main`. Conventional Commits.
