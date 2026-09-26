#!/usr/bin/env bash
# Failover test: runs three engines in one consumer group, streams traffic,
# hard-kills one engine partway through, then checks that its partition moved
# to a survivor with nothing lost and no state applied twice.
#
#   DURATION=3m KILL_AFTER=60 scripts/failover-test.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Keep macOS awake for the whole run (see loadtest.sh).
if [ -z "${LOADTEST_AWAKE:-}" ] && command -v caffeinate >/dev/null; then
  export LOADTEST_AWAKE=1
  exec caffeinate -i "$0" "$@"
fi

RATE=${RATE:-10000}
DURATION=${DURATION:-3m}
KILL_AFTER=${KILL_AFTER:-60}
SEED=${SEED:-$(date +%s)}
VICTIM=engine-2
GROUP=evaluation-engine
TOPIC=platform-telemetry-clickstream
port() { case "$1" in engine) echo 9100 ;; engine-2) echo 9101 ;; engine-3) echo 9102 ;; esac; }

group() {
  docker exec kafka-broker kafka-consumer-groups --bootstrap-server kafka-broker:29092 \
    --describe --group "$GROUP" 2>/dev/null | awk -v t="$TOPIC" '$2 == t'
}
lag() { group | awk '{s += ($6 == "-" ? 0 : $6)} END {print s + 0}'; }
# Maps the group's member hosts (/172.x.x.x) to container names.
names() {
  local args=(-e 's/^//') c ip
  for c in engine engine-2 engine-3; do
    ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$c" 2>/dev/null) || continue
    [ -n "$ip" ] && args+=(-e "s#/$ip #$c #")
  done
  sed "${args[@]}"
}
owners() { group | awk '$7 != "-" {print $7}' | sort -u | wc -l | tr -d ' '; }

echo "== starting 3 engines"
curl -sf "http://localhost:$(port engine)/health" >/dev/null || { echo "run 'make up' first" >&2; exit 1; }
docker compose --profile failover up -d --no-deps --force-recreate engine-2 engine-3 >/dev/null 2>&1
for i in $(seq 1 60); do
  [ "$(owners)" = 3 ] && break
  sleep 2
done
[ "$(owners)" = 3 ] || { echo "partitions never spread over 3 engines" >&2; group; exit 1; }
group | names | sort -k3 | awk '{printf "  partition %s -> %s\n", $3, $8}'
for e in engine engine-2 engine-3; do curl -sf -X POST "http://localhost:$(port $e)/stats/reset"; done

echo
echo "== streaming $RATE events/s for $DURATION (seed $SEED); killing $VICTIM after ${KILL_AFTER}s"
mkdir -p bin data
go build -o bin/generator ./cmd/generator
bin/generator -brokers localhost:9092 -rate "$RATE" -duration "$DURATION" -seed "$SEED" \
  -truth-out data/truth.json > data/failover-generator.log 2>&1 &
gen_pid=$!
sleep "$KILL_AFTER"
kill_time=$(date -u +%Y-%m-%dT%H:%M:%S)
docker kill "$VICTIM" >/dev/null
echo "  $kill_time UTC: killed $VICTIM (SIGKILL, no graceful shutdown)"
wait "$gen_pid"
sent=$(grep '"msg":"done"' data/failover-generator.log | python3 -c 'import sys,json; print(json.load(sys.stdin)["sent"])')
echo "  generator done: $sent events sent"

echo
echo "== waiting for the group to catch up"
for i in $(seq 1 120); do
  [ "$(lag)" = 0 ] && break
  sleep 2
done
final_lag=$(lag)
group | names | sort -k3 | awk '{printf "  partition %s: committed %s of %s (lag %s) -> %s\n", $3, $4, $5, $6, $8}'

echo
echo "== results"
python3 - "$kill_time" "$VICTIM" "$final_lag" <<'PY'
import json, subprocess, sys
from datetime import datetime, timezone
kill_time, victim, lag = sys.argv[1], sys.argv[2], int(sys.argv[3])
kill = datetime.fromisoformat(kill_time).replace(tzinfo=timezone.utc)

takeover = None
for e in ("engine", "engine-3"):
    out = subprocess.run(["docker", "logs", e], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if '"partition assigned"' not in line:
            continue
        t = datetime.fromisoformat(json.loads(line)["time"].replace("Z", "+00:00"))
        if t > kill and (takeover is None or t < takeover[0]):
            takeover = (t, e, json.loads(line)["partition"])

print(f"nothing lost      {'PASS' if lag == 0 else 'FAIL'}  (consumer group lag after drain: {lag})")
if takeover:
    print(f"failover          {(takeover[0] - kill).total_seconds():.1f}s from kill until {takeover[1]} took partition {takeover[2]}")
else:
    print("failover          FAIL  (no survivor was assigned a new partition)")
for e, port in (("engine", 9100), ("engine-3", 9102)):
    s = json.load(__import__("urllib.request").request.urlopen(f"http://localhost:{port}/stats"))
    print(f"{e:17} {s['events']:,} events, {s['duplicates']:,} redelivered, p50 {s['latency_p50_ms']:.1f} ms, "
          f"p99 {s['latency_p99_ms']:.1f} ms, max {s['latency_max_ms']:.0f} ms")
PY

echo
echo "== accuracy (the last line checks for state applied twice)"
docker compose run --rm eval 2>/dev/null | grep -v -E "^\s*$"

echo
echo "== cleanup: stopping the extra engines"
docker compose --profile failover rm -sf engine-2 engine-3 >/dev/null
echo "  done; engine keeps running with all partitions"
