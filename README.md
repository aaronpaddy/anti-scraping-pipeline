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

## Results

Measured 2026-09-26 on a laptop. Everything, including the load generator, ran in one Docker Desktop VM with 10 CPUs and 8 GB. Command: `make loadtest DURATION=10m` at 10,000 events/s.

| Metric | Result |
| --- | --- |
| Throughput | 6,000,007 events in 10 min; held 10k/s with no backlog |
| Latency p50 | 16 ms |
| Latency p99 (whole run) | 115 ms, **target of 50 ms not met** |
| Latency p99 (typical 10 s window) | 25–50 ms |
| AI calls falling back to rules | 3,764 of ~60,000 batches (6%) |
| Stage time per batch | Redis/rules 2.1 ms, AI 6.9 ms, publish 0.3 ms |
| Redis size | 192 MB, 100k keys, flat over the run |

The misses come from spikes every minute or two, when one 10-second window's p99 reaches 100–430 ms. In those windows every stage slows at once, which points to CPU contention in the shared VM rather than one service. Turning off Redis log rewrites didn't help. The next thing to try is running the generator outside the VM, or on another machine.

p99 varies between runs on the same machine: a later 10-minute run measured 344 ms, with 8,714 batches falling back to rules.

Detection accuracy from that later run, which includes the harder personas (see [`spec.md`](spec.md) §4.1). "Alerted" and "Blocked" are the share of users with at least one such decision in 10 minutes:

| Persona | Users | Alerted | Blocked |
| --- | --- | --- | --- |
| human | 42,508 | 12.5% | 7.5% |
| power user | 2,382 | 3.9% | 2.4% |
| scraper | 2,543 | 100% | 100% |
| stealth scraper | 1,478 | 100% | 100% |
| teleporter | 1,089 | 100% | 8.9% |

Every bot type is caught, but user-level precision is only 56% for blocks: about 1 in 13 ordinary humans gets blocked at least once. Each human event has a small chance of scoring above the block threshold (about 0.3% offline), and a human sends dozens of events in 10 minutes. Blocking on a single event is too aggressive.
