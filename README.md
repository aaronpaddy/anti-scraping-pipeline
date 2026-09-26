# Real-Time Telemetry & Anti-Scraping Pipeline

Streams simulated clickstream events through Kafka and flags scraping bots in real time. A Go engine runs a location-speed check against per-user state in Redis. A Python service scores behavior with a k-NN search in Qdrant. See [`spec.md`](spec.md) for the design.

```
generator ─► Kafka ─► Go engine ─► Kafka (alerts) ─► Pinot
                        │   ▲
                 Redis ◄┘   └─► AI engine (FastAPI) ─► Qdrant
```

## Requirements

- Docker with Compose v2 (for the full pipeline)
- Go 1.27+ and Python 3.13 (for tests and local builds)

## Run it

```sh
make up                  # Kafka, Redis, Qdrant (+ seeding), AI engine, Go engine
make gen                 # 10k events/s for 1 minute (RATE=, DURATION=, SEED=)
make eval                # accuracy per persona for that run
make stats               # engine counters and latency
make loadtest DURATION=10m   # sustained run, then latency + accuracy report
make loadtest-local DURATION=10m  # same, with the generator outside Docker
make failover DURATION=3m    # 3 engines; one is killed mid-stream
make analytics           # Pinot at http://localhost:9000; queries in pinot/queries.sql
make down                # stop; `make reset` also deletes volumes
```

## Tests

```sh
make test                # go vet, go test -race, pytest
```

The Go suite includes an end-to-end test of the consumer. It runs against an in-process Kafka (`kfake`), an in-process Redis (`miniredis`) and a stubbed AI engine, and covers batching, per-user ordering, alert production, and offset commits across a restart. Processor tests cover redelivery: a replayed batch repeats its decisions without re-applying state. The Python suite tests the Qdrant REST calls against a mocked transport.

`BENCH_REDIS_ADDR=localhost:6379 BENCH_REDIS_PASSWORD=dev_pipeline_pass_99 go test -run x -bench PrepareRedis ./internal/engine/` benchmarks stage 1 against the running Redis.

## Layout

| Path | What |
| --- | --- |
| `cmd/engine` | Evaluation engine: Kafka consumer, `GET /stats`, `POST /stats/reset` |
| `cmd/generator` | Seeded traffic generator; writes `data/truth.json` |
| `cmd/seed-export` | Builds labeled feature vectors for Qdrant |
| `cmd/eval` | Reads the alerts topic and reports accuracy per persona |
| `internal/detect` | Velocity check, activity window + features, decision policy |
| `internal/engine` | Batch processor, per-partition consumer, AI client |
| `internal/store` | Redis state (and an in-memory version for tests) |
| `internal/gen` | Traffic simulator (human, power user, scraper, stealth scraper, teleporter) |
| `ai/` | FastAPI AI engine and Qdrant seeding script |
| `pinot/` | Pinot schema, real-time table config, sample queries |

## Engine settings

Every flag also reads an environment variable (used by Compose):

| Env var | Default | Meaning |
| --- | --- | --- |
| `BATCH_SIZE` | 100 | Max events per batch |
| `BATCH_WINDOW` | 10ms | Max wait to fill a batch |
| `AI_TIMEOUT` | 20ms | AI call budget; on timeout the batch uses rules only |
| `AI_URL` | `http://localhost:8000` | Empty string runs rules only |
| `WORKERS` | 4 | Goroutines per batch (users sharded by hash) |
| `SESSION_TIMEOUT` | 10s | How long before a silent engine's partitions move to the others |
| `AI_WORKERS` | 4 | AI engine worker processes (Compose variable, passed to uvicorn as `WEB_CONCURRENCY`) |

## Results

Measured 2026-09-26 on a MacBook with an Apple M4 (4 performance + 6 efficiency cores). Docker Desktop had all 10 cores and 8 GB. Command: `make loadtest-local DURATION=10m` at 10,000 events/s, with the generator running natively outside the Docker VM.

| Metric | Result |
| --- | --- |
| Throughput | 5,999,994 events in 10 min; held 10k/s with no backlog |
| Latency p50 | 16 ms |
| Latency p99 (whole run) | 98 ms, **target of 50 ms not met** |
| Latency max | 377 ms |
| 10 s windows with p99 over 50 ms | 32 of 61 (8 over 100 ms) |
| AI calls falling back to rules | 5,284 of ~60,000 batches (9%) |
| Stage time per batch | Redis/rules 2.3 ms, AI 6.8 ms, publish 0.3 ms |
| Redis size | ~200 MB, 100k keys, flat over the run |

The misses come from short spikes in which every stage slows at once, which points to CPU contention in the shared VM rather than one service. With the generator inside the VM, whole-run p99 was 115–344 ms across runs. Moving it out helped, but Kafka, Redis, Qdrant, the AI workers and the engine still share the same cores, half of which are efficiency cores. Turning off Redis log rewrites made no difference. Cutting the AI engine from 4 workers to 2 (`AI_WORKERS=2`) made things worse: p99 300 ms, with 9,533 batches falling back to rules. Meeting 50 ms likely needs the pipeline on dedicated hardware.

Detection accuracy over 10 minutes, including the harder personas (see [`spec.md`](spec.md) §4.1). "Blocked" is the share of users blocked at least once. The two columns compare the old rule (block on one score ≥ 0.95) with the current one (block only when 8 of the user's last 10 scores are high; see §4.5):

| Persona | Users | Blocked, old rule | Blocked, current rule |
| --- | --- | --- | --- |
| human | ~42,500 | 7.5% | **0.7%** |
| power user | ~2,400 | 2.4% | **0%** |
| scraper | ~2,500 | 100% | 100% |
| stealth scraper | ~1,470 | 100% | 100% |
| teleporter | ~1,050 | 8.9% | 0.8% (all flagged by the velocity check) |

With the current rule, 93.4% of blocked users are bots, up from 56%. The cost is speed: offline replay puts the median time to block a stealth scraper at about 107 s, up from 38 s. The humans still blocked browse many distinct profiles with fairly regular timing, so they look like stealth scrapers; reducing them further needs better features.

### Failover

`make failover` runs three engines in one consumer group, streams 10,000 events/s for 3 minutes, and hard-kills one engine (`docker kill`, no graceful shutdown) after 60 seconds.

| Check | Result |
| --- | --- |
| Nothing lost | Every partition fully committed after the run (consumer group lag 0, 1.8M events) |
| Failover time | 9.6 s from the kill until a survivor took the dead engine's partition; the other partitions didn't move (cooperative-sticky rebalancing) |
| Redelivery | The survivor re-received 11,113 events the dead engine had processed but not committed, and recognized each one |
| State applied twice | None: 0 velocity alerts for users who never move |
| Duplicate alerts | 3,869, from batches published but not committed before the kill (at-least-once delivery) |
| Latency | The unaffected engine stayed at p99 97 ms; the engine that took over peaked at 4 s while catching up |

Recent-score history (used by the block rule) is kept in memory per partition, so users on the moved partition start over. They are still flagged, but can take longer to block.
