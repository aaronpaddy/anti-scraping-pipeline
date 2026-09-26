# System Architecture Specification & Design Document (SSD)

## Project Name: Real-Time Platform Telemetry & Anti-Scraping Pipeline

**Target Engineering Track:** Distributed Systems & Data Infrastructure (Intern / New Grad)

**Scale Objective:** Multi-threaded streaming engine processing 10,000+ telemetry events per second (EPS), with a p99 processing latency under 50ms, measured from consumer poll to alert produce.

---

## 1. Executive Summary & Tech Stack

This project implements an asynchronous, decoupled set of services that ingest real-time user clickstreams and flag profile-scraping bots. It moves bot detection out of delayed batch post-processing and into the stream itself.

### Tech Stack

*   **Message Broker:** Apache Kafka in KRaft mode (no ZooKeeper).
*   **Session State Cache:** Redis 7.2 — per-user last location, recent-activity window, and event de-duplication.
*   **Core Backend Engine:** Go (goroutine worker pool, `franz-go` Kafka client).
*   **Telemetry Generator:** Go (same toolchain as the backend; easy to push 10k+ EPS).
*   **AI Risk Engine:** Python + FastAPI (async).
*   **Vector Database:** Qdrant v1.19 (k-NN cosine search over behavior embeddings).
*   **Analytics Store:** Apache Pinot (real-time ingestion from the alerts topic).
*   **Infrastructure:** Docker & Docker Compose.

---

## 2. System Topology & Data Flow

Telemetry generation, rule-based detection, and AI behavior scoring run as separate services connected by Kafka and HTTP.

```
┌─────────────────────────┐
│   Telemetry Generator   │  (Simulated humans + bot personas, 10k+ EPS)
└────────────┬────────────┘
             │ produce (key = viewer_user_id)
             ▼
┌─────────────────────────┐
│   Kafka Broker          │  topic: platform-telemetry-clickstream (3 partitions)
└────────────┬────────────┘
             │ poll + micro-batch
             ▼
┌─────────────────────────┐        ┌───────────────────────────┐
│   Go Evaluation Engine  │ ─────► │ Redis                     │
│                         │ ◄───── │ (last location, recent    │
└────────────┬────────────┘        │  activity window)         │
             │                     └───────────────────────────┘
             │ POST /evaluate/batch
             ▼
┌─────────────────────────┐        ┌───────────────────────────┐
│   Python AI Risk Engine │ ─────► │ Qdrant                    │
│   (FastAPI)             │ ◄───── │ (labeled behavior vectors)│
└────────────┬────────────┘        └───────────────────────────┘
             │ scores back to Go engine, which produces decisions
             ▼
┌─────────────────────────┐
│   Kafka Broker          │  topic: telemetry-anomaly-alerts (3 partitions)
└────────────┬────────────┘
             │ real-time ingestion
             ▼
┌─────────────────────────┐
│   Apache Pinot          │  (dashboards / analytics queries)
└─────────────────────────┘
```

---

## 3. Interface & Payload Contracts

Payload formats are fixed at every service boundary.

### 3.1 Inbound Raw Telemetry (`platform-telemetry-clickstream`)

```json
{
  "event_id": "8f3b2a9c-7e1b-4d5a-9f2c-3a4b5c6d7e8f",
  "viewer_user_id": "usr_bot_9984120",
  "target_profile_id": "usr_prof_123456",
  "timestamp": 1790342400000,
  "action_type": "PROFILE_VIEW",
  "location_coordinates": {
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "device_signature": "mozilla/5.0_macos_apple_webkit"
}
```

*   `timestamp` is Unix epoch milliseconds, set by the client.
*   Message key = `viewer_user_id`.

### 3.2 Outbound Anomaly Decision (`telemetry-anomaly-alerts`)

Only events whose decision is not `ALLOW` are published.

```json
{
  "event_id": "8f3b2a9c-7e1b-4d5a-9f2c-3a4b5c6d7e8f",
  "viewer_user_id": "usr_bot_9984120",
  "event_timestamp": 1790342400000,
  "evaluated_at": 1790342400011,
  "geographic_velocity_triggered": true,
  "ai_scraping_anomaly_score": 0.965,
  "ai_score_available": true,
  "action_taken": "BLOCK_SESSION",
  "pipeline_processing_duration_ms": 11
}
```

*   `action_taken` is one of `ALLOW`, `FLAG_FOR_REVIEW`, `BLOCK_SESSION` (see §4.5).
*   `ai_score_available` is `false` when the AI engine timed out or failed, or the user has fewer than 5 events of history; `ai_scraping_anomaly_score` is then `null`.
*   `event_timestamp` is the source event's `timestamp`; `evaluated_at` (epoch ms) is when the decision was made and is Pinot's time column.
*   `pipeline_processing_duration_ms` is measured from when the engine polled the event to when the decision is handed to the producer.
*   Message key = `viewer_user_id`.

### 3.3 AI Engine API (`POST /evaluate/batch`)

The Go engine sends one request per micro-batch, never one per event.

```json
// request
{ "items": [ { "event_id": "…", "viewer_user_id": "…", "features": [0.12, 0.98, …] } ] }

// response
{ "results": [ { "event_id": "…", "anomaly_score": 0.965 } ] }
```

---

## 4. Component Specifications

### 4.1 Telemetry Generator

*   **Purpose:** Load-test the system with realistic traffic in the exact format of §3.1.
*   **Behavior:** Seeded random generator (same seed produces the same stream) running concurrent producer goroutines. Target rate is configurable.
*   **Personas** (default share of users):

    | Persona | Share | Timing | Profiles viewed | Counts as |
    | --- | --- | --- | --- | --- |
    | Human | ~85% | Random gaps, mean 10s | Mostly 3–15 favorites | Human |
    | Power user | 5% | Random gaps, mean 4s | Mostly new profiles, some revisits | Human |
    | Scraper | 5% | Every ~1s (±10%) | A new profile every time, sequential | Bot |
    | Stealth scraper | 3% | Uniform random 3–12s sleeps, rare 20–45s pauses | 85% new, 15% revisits | Bot |
    | Teleporter | 2% | Human timing | Human pattern, but jumps between cities | Bot |

    Scrapers are easy to catch. The hard cases are stealth scrapers against power users, which look alike on rate and distinct profiles. The stealth scraper's only remaining tell is that uniform random sleeps are more regular than human timing.
*   Each user's persona is written to a truth file (`data/truth.json`), never into the payload, so detection accuracy can be measured. User ids include the seed (`usr_s42_0000123`), so runs with different seeds never share state.

### 4.2 Kafka

*   **Topics:** `platform-telemetry-clickstream` and `telemetry-anomaly-alerts`, 3 partitions each, created explicitly at startup (auto-create is disabled).
*   **Partition key:** `viewer_user_id`, so all of a user's events land on one partition, in order.
*   **Producer durability:** `acks=1` — faster, and acceptable here because a lost telemetry event is low-cost. (With one local broker, `acks=all` behaves the same.)
*   **Failure detection:** The group's session timeout is 10s (`SESSION_TIMEOUT`; the client default is 45s). A crashed engine's partitions move to the survivors about 10s after it stops heartbeating. Cooperative-sticky rebalancing leaves the survivors' own partitions in place. A failover test is in `scripts/failover-test.sh`.
*   **Delivery semantics:** at-least-once. The consumer marks offsets only after a batch's alerts are acknowledged, and commits marked offsets every second. A crash therefore replays at most about a second of work: in the failover test, 900 redelivered events and 3 repeated alerts, versus 11,113 and 3,869 with a 5-second interval. A redelivered event never changes state twice, but its alert can be published again (§4.3).

### 4.3 Go Evaluation Engine

*   **Purpose:** Poll Kafka, run rule-based checks against Redis state, collect AI scores, and publish decisions.
*   **Micro-batching:** Flush a batch at 100 events or 10ms, whichever comes first.
*   **Pipelining:** Each partition runs two stages. Stage 1 loads state, runs the rules and saves state; stage 2 calls the AI engine, publishes alerts and commits. Batch N+1's stage 1 overlaps batch N's stage 2, and both stages run batches in order.
*   **Ordering:** A batch is fanned out to N worker goroutines by `hash(viewer_user_id) % N`. One user's events are always handled by one worker, in order.
*   **State cache:** Each partition's worker keeps its users' state in memory and writes every change through to Redis. It reads Redis only for users it hasn't seen since the partition was assigned. This is safe because a user's events all go to one partition, so that worker is the only writer.
*   **Redelivery:** Each activity entry records its event's velocity result. An event already in its user's window is a redelivery: it reuses the stored result, leaves state alone, and is scored and published again. Kafka redelivers within seconds, well inside the window (5 min / 200 events).

**Redis keys (per user):**

| Key | Type | Contents | TTL |
| --- | --- | --- | --- |
| `session:v1:{viewer_user_id}` | Hash | `last_latitude`, `last_longitude`, `last_timestamp` | 30 min |
| `activity:v1:{viewer_user_id}` | Sorted set (score = timestamp) | Members `event_id\|target_profile_id\|velocity`, trimmed to the last 5 min / 200 entries | 30 min |

An earlier design used one `dedup:v1:{event_id}` key per event. At 10k events/sec that meant millions of keys and two extra writes per event. It stalled Redis in a 10-minute load test, so the activity window now does this job.

**Geographic velocity check:** For event N, read the previous location and timestamp from `session:v1`.

*   Compute distance d (miles, haversine) and time gap Δt.
*   Trigger (`geographic_velocity_triggered: true`) when d > 5 miles **and** d / Δt > 600 mph. The 5-mile floor ignores GPS/IP jitter; 600 mph is roughly commercial-jet speed.
*   Δt ≤ 0 (same millisecond or out of order) with d > 5 miles also triggers.
*   First event for a user (no previous state): no trigger; just store the state.
*   The new location is written to Redis **immediately after the check**, before the AI call, so the next event from that user sees fresh state. Events older than `last_timestamp` do not overwrite it.

**Feature extraction:** After updating `activity:v1`, the engine computes a 5-dimension vector from the user's recent-activity window, each value scaled to [0, 1]: request rate, mean gap between actions, gap regularity (coefficient of variation), share of distinct target profiles, and window size. Users with fewer than 5 events in the window are not sent to the AI engine.

### 4.4 Python AI Risk Engine

*   **Purpose:** Score how bot-like a user's recent behavior is.
*   **Framework:** FastAPI, async, batch endpoint only (§3.3).
*   **Qdrant index:** Holds labeled reference vectors from generated traffic, built with a different seed from the one used for evaluation. There are 10,000 vectors per persona: humans and power users are labeled `human`, scrapers and stealth scrapers `bot`. Each persona is sampled separately, so slow stealth scrapers aren't drowned out by fast scrapers. Teleporters are left out: their timing is human, and the velocity check is what catches them.
*   **Scoring:** For each item, run a k-NN cosine search (k = 10). The anomaly score is the similarity-weighted share of `bot` neighbors, from 0 to 1.
*   **Speed:** The seeding job builds Qdrant's HNSW index before the service starts; at ~1 MB the collection is below Qdrant's default indexing threshold and would otherwise be brute-force scanned. Queries use `hnsw_ef` 32 and go to Qdrant's REST API directly, skipping qdrant-client's response models. A 100-item batch takes about 9ms.
*   **Timeout:** The Go engine waits at most 20ms per batch. On timeout or error, it proceeds with the rule-based result alone (`ai_score_available: false`).

### 4.5 Decision Policy

A block needs **sustained** evidence: at least 8 of the user's last 10 AI scores are ≥ 0.85, counting the current one. The engine keeps each user's last 10 scores in memory, per partition.

| Velocity triggered | AI score (this event) | Sustained | Action |
| --- | --- | --- | --- |
| any | ≥ 0.95 | yes | `BLOCK_SESSION` |
| yes | ≥ 0.85 | yes | `BLOCK_SESSION` |
| yes | anything else | – | `FLAG_FOR_REVIEW` |
| no | ≥ 0.85 | no, or score < 0.95 | `FLAG_FOR_REVIEW` |
| no | < 0.85 or unavailable | – | `ALLOW` |

Velocity alone never blocks a session, because VPNs and mobile carriers produce false location jumps.

A single high score only flags. An earlier version blocked on one score ≥ 0.95, which blocked 7.5% of simulated humans at least once in 10 minutes. Each human event has only a ~0.3% chance of scoring that high, but a human sends dozens of events. Replaying a 10-minute simulated run offline against several rules gave:

| Block rule | Humans blocked | Power users blocked | Stealth scrapers blocked (median time) | Scrapers blocked (median time) |
| --- | --- | --- | --- | --- |
| One score ≥ 0.95 | 7.3% | 2.8% | 100% (38 s) | 100% (4 s) |
| 3 of last 5 | 2.6% | 0.0% | 100% (66 s) | 100% (8 s) |
| **8 of last 10** (chosen) | **0.6%** | **0.0%** | **100% (107 s)** | **100% (13 s)** |

The humans still blocked under the chosen rule browse many distinct profiles with fairly regular timing, so they look like stealth scrapers. Tightening the rule further mainly slows bot detection; reducing them further needs better features, not a stricter rule.

### 4.6 Analytics (Apache Pinot)

*   A Pinot real-time table ingests `telemetry-anomaly-alerts` directly from Kafka.
*   Used for dashboards: alerts per minute, action breakdown, p99 `pipeline_processing_duration_ms`.

---

## 5. Event Lifecycle

```
[Generator]   [Kafka]      [Go Engine]              [Redis]        [AI Engine]   [Qdrant]
    │            │              │                       │               │            │
    │──produce──>│              │                       │               │            │
    │            │──poll batch─>│                       │               │            │
    │            │              │──read state (cache────>│               │            │
    │            │              │  misses only)         │               │            │
    │            │              │<──────────────────────│               │            │
    │            │              │  [velocity check]     │               │            │
    │            │              │──write location +────>│               │            │
    │            │              │  add to activity      │               │            │
    │            │              │  [build features]     │               │            │
    │            │              │──POST /evaluate/batch────────────────>│            │
    │            │              │                       │               │──k-NN─────>│
    │            │              │                       │               │<───────────│
    │            │              │<──────────────scores (≤20ms)──────────│            │
    │            │              │  [decision policy]    │               │            │
    │            │<─alerts──────│                       │               │            │
    │            │<─commit──────│                       │               │            │
```

---

## 6. Local Infrastructure

The full stack is defined in `docker-compose.yml`. Credentials there are local-development values only.

| Service | Image | Role |
| --- | --- | --- |
| `kafka-broker` | `confluentinc/cp-kafka:7.5.0` | Single-node Kafka in KRaft mode; auto topic creation disabled |
| `kafka-init` | same | One-off: creates both topics with 3 partitions |
| `redis-cache` | `redis:7.2-alpine` | Session state and activity windows (AOF on, RDB snapshots off) |
| `qdrant` | `qdrant/qdrant:v1.19.1` | Vector index |
| `seed-export` → `qdrant-seed` | project images | One-off: builds labeled vectors, loads them into Qdrant |
| `ai-engine` | project image (Python) | FastAPI scoring service |
| `engine` | project image (Go) | Evaluation engine; `GET /stats` on port 9100 |
| `generator`, `eval` | project image (Go) | Profile `load`: traffic and accuracy report |
| `pinot`, `pinot-init` | `apachepinot/pinot:1.5.1` | Profile `analytics`: real-time table over the alerts topic |
| `prometheus`, `grafana`, `kafka-exporter` | `prom/prometheus:v3.15.0`, `grafana/grafana:13.2.2`, `danielqsj/kafka-exporter:v1.10.0` | Profile `monitoring`: engine metrics, consumer lag, and a provisioned dashboard |
| `engine-2`, `engine-3` | project image (Go) | Profile `failover`: extra engines in the same consumer group |
