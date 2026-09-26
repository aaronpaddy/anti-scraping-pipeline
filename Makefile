RATE     ?= 10000
DURATION ?= 1m
SEED     ?= 42
COMPOSE  := docker compose

.PHONY: test test-go test-ai venv build up down reset logs gen eval stats loadtest loadtest-local failover monitoring analytics

test: test-go test-ai

test-go:
	go vet ./...
	go test -race ./...

ai/.venv:
	python3 -m venv ai/.venv
	ai/.venv/bin/pip install -q -r ai/requirements-dev.txt

venv: ai/.venv

test-ai: ai/.venv
	cd ai && .venv/bin/python -m pytest -q

build:
	go build -o bin/ ./cmd/...

# Start the pipeline (Kafka, Redis, Qdrant + seeding, AI engine, Go engine).
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) --profile load --profile analytics --profile failover --profile monitoring down

# Also deletes Kafka, Redis, Qdrant and seed volumes.
reset:
	$(COMPOSE) --profile load --profile analytics --profile failover --profile monitoring down -v

logs:
	$(COMPOSE) logs -f engine ai-engine

# Stream simulated traffic: make gen RATE=10000 DURATION=1m SEED=42
gen:
	RATE=$(RATE) DURATION=$(DURATION) SEED=$(SEED) $(COMPOSE) run --rm generator

# Accuracy report for the last `make gen` run.
eval:
	$(COMPOSE) run --rm eval

stats:
	@curl -s localhost:9100/stats; echo

# Sustained load test: make loadtest RATE=10000 DURATION=10m
loadtest:
	RATE=$(RATE) DURATION=$(DURATION) scripts/loadtest.sh

# Same, with the generator running natively outside the Docker VM.
loadtest-local:
	LOCAL_GEN=1 RATE=$(RATE) DURATION=$(DURATION) scripts/loadtest.sh

# Kill one of three engines mid-stream and check nothing is lost:
# make failover DURATION=3m KILL_AFTER=60
failover:
	RATE=$(RATE) DURATION=$(DURATION) KILL_AFTER=$(or $(KILL_AFTER),60) scripts/failover-test.sh

# Start Prometheus + Grafana; dashboard at http://localhost:3000
monitoring:
	$(COMPOSE) --profile monitoring up -d prometheus kafka-exporter grafana

# Start Pinot and register the alerts table (UI at http://localhost:9000).
analytics:
	$(COMPOSE) --profile analytics up -d pinot pinot-init
