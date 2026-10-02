#!/usr/bin/env bash
# Real-client check: kcat (librdkafka) produces, the broker restarts, kcat consumes,
# a topic is branched and replayed, and plain git sees it all. Requires: kcat, git, cargo.
# Set KCAT to use a kcat/kafkacat binary that is not on PATH as `kcat`.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
PORT="${KOMMIT_E2E_PORT:-19092}"
BROKER="127.0.0.1:${PORT}"
KCAT="${KCAT:-kcat}"
PID=""
cleanup() {
  rc=$?
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  if [ "$rc" -ne 0 ] && [ -f "$WORK/broker.log" ]; then
    echo "--- broker log"; cat "$WORK/broker.log"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

cargo build --quiet --manifest-path "$ROOT/Cargo.toml"
BIN="$ROOT/target/debug/kommit"

start() {
  RUST_LOG="${RUST_LOG:-kommit=debug}" "$BIN" --data "$WORK/data.git" --listen "$BROKER" \
    --advertised-host 127.0.0.1 --group-initial-rebalance-delay-ms 300 >>"$WORK/broker.log" 2>&1 &
  PID=$!
  for _ in $(seq 50); do
    "$KCAT" -b "$BROKER" -L -m 1 >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "broker did not come up"; cat "$WORK/broker.log"; exit 1
}

stop() { kill "$PID"; wait "$PID" 2>/dev/null || true; PID=""; }

start
printf 'hello\nworld\n' | "$KCAT" -b "$BROKER" -P -t orders -p 0
printf 'zipped\n' | "$KCAT" -b "$BROKER" -P -t orders -p 0 -z gzip
stop
start

got="$("$KCAT" -b "$BROKER" -C -t orders -p 0 -o beginning -e -q)"
want=$'hello\nworld\nzipped'
[ "$got" = "$want" ] || { echo "consume mismatch:"; echo "$got"; cat "$WORK/broker.log"; exit 1; }

log="$(git --git-dir="$WORK/data.git" log --format=%s orders/0)"
want_log=$'zipped\nworld\nhello\nkommit: partition orders/0 created'
[ "$log" = "$want_log" ] || { echo "git log mismatch:"; echo "$log"; exit 1; }

git --git-dir="$WORK/data.git" fsck --strict --no-dangling

git init --quiet --bare "$WORK/remote.git"
git --git-dir="$WORK/data.git" push --quiet --all "$WORK/remote.git"
[ "$(git --git-dir="$WORK/remote.git" log -1 --format=%s orders/0)" = "zipped" ]

# Consumer groups: kcat -G joins, consumes everything, and commits on close.
# The committed offset is a ref, so git itself can measure the lag.
timeout 60 "$KCAT" -b "$BROKER" -G e2e-group -X auto.offset.reset=earliest -c 3 -q orders >"$WORK/group.out"
[ "$(cat "$WORK/group.out")" = "$want" ] || { echo "group consume mismatch:"; cat "$WORK/group.out"; exit 1; }
lag="$(git --git-dir="$WORK/data.git" rev-list --count refs/groups/e2e-group/orders/0..orders/0)"
[ "$lag" = "0" ] || { echo "expected lag 0, git says $lag"; exit 1; }

# Branching (M3): fork at a timestamp, replay the fork with a fresh group, diverge it.
printf 'old1\nold2\n' | "$KCAT" -b "$BROKER" -P -t events -p 0
sleep 1.1
cut="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
sleep 1.1
printf 'new1\n' | "$KCAT" -b "$BROKER" -P -t events -p 0
"$BIN" branch --bootstrap "$BROKER" --at "$cut" events events-replay

got="$("$KCAT" -b "$BROKER" -C -t events-replay -p 0 -o beginning -e -q)"
[ "$got" = $'old1\nold2' ] || { echo "fork replay mismatch:"; echo "$got"; exit 1; }
timeout 60 "$KCAT" -b "$BROKER" -G replay-group -X auto.offset.reset=earliest -c 2 -q events-replay >"$WORK/replay.out"
[ "$(cat "$WORK/replay.out")" = $'old1\nold2' ] || { echo "fork group mismatch:"; cat "$WORK/replay.out"; exit 1; }

printf 'diverged\n' | "$KCAT" -b "$BROKER" -P -t events-replay -p 0
got="$("$KCAT" -b "$BROKER" -C -t events -p 0 -o beginning -e -q)"
[ "$got" = $'old1\nold2\nnew1' ] || { echo "source changed:"; echo "$got"; exit 1; }

g() { git --git-dir="$WORK/data.git" "$@"; }
[ "$(g rev-parse events-replay/0~1)" = "$(g rev-parse events/0~1)" ] || { echo "fork does not share commits"; exit 1; }
[ "$(g merge-base events/0 events-replay/0)" = "$(g rev-parse events/0~1)" ] || { echo "unexpected merge base"; exit 1; }
g fsck --strict --no-dangling

echo "e2e-kcat: OK"
