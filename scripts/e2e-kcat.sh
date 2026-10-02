#!/usr/bin/env bash
# Real-client check: kcat (librdkafka) produces, the broker restarts, kcat consumes,
# and plain git sees the topic. Requires: kcat, git, cargo.
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
    --advertised-host 127.0.0.1 >>"$WORK/broker.log" 2>&1 &
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

echo "e2e-kcat: OK"
