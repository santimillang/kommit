#!/usr/bin/env bash
# Real Java clients against kommit, in Docker: kafka-topics creates a topic, the console
# producer (idempotent by default) writes to it, a console consumer group reads it and
# commits, kafka-consumer-groups reports the lag, and plain git agrees on the host. Then
# `kommit branch` forks the topic and a fresh Java consumer group replays the fork.
# Requires: docker, git, cargo. KAFKA_IMAGE picks the Kafka tools image.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
NET="kommit-e2e-$$"
NAME="kommit-e2e-$$"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.1.0}"
BOOTSTRAP="kommit:9092"
# A throwaway docker config, so local credential helpers are never involved.
export DOCKER_CONFIG="$WORK/docker"
mkdir -p "$DOCKER_CONFIG" "$WORK/data"
echo '{}' >"$DOCKER_CONFIG/config.json"

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "--- broker log"; docker logs "$NAME" 2>&1 | tail -50 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

cargo build --quiet --manifest-path "$ROOT/Cargo.toml"

docker network create "$NET" >/dev/null
docker run -d --name "$NAME" --network "$NET" --network-alias kommit \
  --user "$(id -u):$(id -g)" \
  -e RUST_LOG=kommit=debug \
  -v "$ROOT/target/debug/kommit:/kommit:ro" -v "$WORK/data:/data" \
  ubuntu:24.04 \
  /kommit --data /data/kommit.git --listen 0.0.0.0:9092 --advertised-host kommit \
  --group-initial-rebalance-delay-ms 300 >/dev/null

kafka() { docker run --rm -i --network "$NET" "$KAFKA_IMAGE" "$@"; }

for _ in $(seq 60); do
  kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1 && break
  sleep 1
done

kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --topic orders --partitions 2

seq 1 10 | kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic orders

consume() {
  kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
    --topic orders --group java-group --from-beginning \
    --consumer-property group.protocol=classic \
    --max-messages "$1" --timeout-ms 60000
}

got="$(consume 10 | sort -n | tr '\n' ' ')"
[ "$got" = "1 2 3 4 5 6 7 8 9 10 " ] || { echo "first consume got: $got"; exit 1; }

# The group resumes from its committed offsets: only new records arrive.
printf '11\n12\n' | kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic orders
got="$(consume 2 | sort -n | tr '\n' ' ')"
[ "$got" = "11 12 " ] || { echo "second consume got: $got"; exit 1; }

describe="$(kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" \
  --describe --group java-group)"
echo "$describe"
# Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
echo "$describe" | awk '$1 == "java-group" && $2 == "orders" { if ($6 != "0") bad = 1; n++ }
  END { exit (n == 2 && !bad) ? 0 : 1 }' || { echo "kafka-consumer-groups does not report lag 0"; exit 1; }

for p in 0 1; do
  lag="$(git --git-dir="$WORK/data/kommit.git" rev-list --count "refs/groups/java-group/orders/$p..orders/$p")"
  [ "$lag" = "0" ] || { echo "git lag on partition $p: $lag"; exit 1; }
done

# Branching (M3). kafka-topics.sh validates config names client-side
# (LogConfig.validateNames), so it can never send kommit.branch.*: pin that.
if out="$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create \
    --topic orders-replay --config kommit.branch.from=orders 2>&1)"; then
  echo "kafka-topics.sh accepted kommit.branch.from; update the docs"; exit 1
fi
echo "$out" | grep -q "Unknown topic config name: kommit.branch.from" \
  || { echo "unexpected kafka-topics.sh failure: $out"; exit 1; }

# The kommit CLI branches over the Admin API, and a fresh Java group replays the fork.
docker run --rm --network "$NET" --user "$(id -u):$(id -g)" \
  -v "$ROOT/target/debug/kommit:/kommit:ro" ubuntu:24.04 \
  /kommit branch --bootstrap "$BOOTSTRAP" orders orders-replay
got="$(kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --topic orders-replay --group replay-group --from-beginning \
  --consumer-property group.protocol=classic \
  --max-messages 12 --timeout-ms 60000 | sort -n | tr '\n' ' ')"
[ "$got" = "1 2 3 4 5 6 7 8 9 10 11 12 " ] || { echo "fork replay got: $got"; exit 1; }
d="$WORK/data/kommit.git"
for p in 0 1; do
  [ "$(git --git-dir="$d" rev-parse "orders-replay/$p")" = "$(git --git-dir="$d" rev-parse "orders/$p")" ] \
    || { echo "fork partition $p does not share the source head"; exit 1; }
done

echo "e2e-java: OK"
