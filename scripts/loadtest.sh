#!/usr/bin/env bash
# Runs the generator at a fixed rate against the running pipeline, then reports
# the engine's throughput and latency and the detection accuracy.
#
#   RATE=10000 DURATION=10m scripts/loadtest.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# On macOS, keep the machine awake for the whole run: an idle sleep freezes
# Docker mid-test and wrecks the latency numbers.
if [ -z "${LOADTEST_AWAKE:-}" ] && command -v caffeinate >/dev/null; then
  export LOADTEST_AWAKE=1
  exec caffeinate -i "$0" "$@"
fi

RATE=${RATE:-10000}
DURATION=${DURATION:-10m}
SEED=${SEED:-$(date +%s)} # fresh users each run, so no state carries over
STATS=${STATS:-http://localhost:9100/stats}

curl -sf "$STATS" >/dev/null || { echo "engine not reachable at $STATS; run 'make up' first" >&2; exit 1; }

echo "== load test: $RATE events/s for $DURATION (seed $SEED)"
curl -sf -X POST "$STATS/reset"
start=$(date +%s)
RATE=$RATE DURATION=$DURATION SEED=$SEED docker compose run --rm generator

# Wait for the engine to drain what the generator sent.
prev=-1
while true; do
  sleep 3
  n=$(curl -sf "$STATS" | python3 -c 'import sys,json; print(json.load(sys.stdin)["events"])')
  [ "$n" = "$prev" ] && break
  prev=$n
done
elapsed=$(( $(date +%s) - start ))

echo
echo "== engine"
curl -sf "$STATS" | python3 -c "
import sys, json
s = json.load(sys.stdin)
print(f\"events       {s['events']:,}  (~{s['events'] / $elapsed:,.0f}/s over ${elapsed}s incl. drain)\")
print(f\"latency      p50 {s['latency_p50_ms']:.1f} ms   p99 {s['latency_p99_ms']:.1f} ms   max {s['latency_max_ms']:.1f} ms\")
print(f\"target       p99 < 50 ms: {'PASS' if s['latency_p99_ms'] < 50 else 'FAIL'}\")
print(f\"actions      {s['actions']}\")
print(f\"ai           {s['ai_scored']:,} scored, {s['ai_failed_batches']} batches fell back to rules\")
print(f\"stages       prepare {s['avg_prepare_ms']:.1f} ms, ai {s['avg_ai_ms']:.1f} ms, produce {s['avg_produce_ms']:.1f} ms (avg per batch)\")
print(f\"redelivered  {s['duplicates']}   bad records {s['bad_records']}\")
"

echo
echo "== accuracy"
docker compose run --rm eval
