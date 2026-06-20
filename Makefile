COMPOSE_DIR     := infra
COMPOSE_FILE    := $(COMPOSE_DIR)/docker-compose.yaml
ENV_FILE        := .env

COMPOSE         := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) --project-directory $(COMPOSE_DIR)

.DEFAULT_GOAL   := help

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "  NeuralSentinel — available targets"
	@echo ""
	@echo "  Stack control"
	@echo "    up           Start all services (detached)"
	@echo "    down         Stop and remove containers"
	@echo "    volume-down      down + remove named volumes (DESTRUCTIVE)"
	@echo "    restart      down + up"
	@echo "    stop         Stop containers without removing them"
	@echo "    start        Start already-created containers"
	@echo ""
	@echo "  Observability"
	@echo "    ps           Show running containers + health"
	@echo "    logs         Tail logs for all services"
	@echo "    logs-kafka   Tail Kafka broker logs"
	@echo "    logs-pg      Tail Postgres logs"
	@echo "    logs-redis   Tail Redis logs"
	@echo "    logs-mlflow  Tail MLflow logs"
	@echo ""
	@echo "  Kafka"
	@echo "    topics-sync   Create/update topics from topics.yaml (idempotent)"
	@echo "    topics-delete Delete all declared topics (DESTRUCTIVE)"
	@echo "    topics-list   List Kafka topics on the cluster"
	@echo ""
	@echo "  Maintenance"
	@echo "    clean        down + remove named volumes (DESTRUCTIVE)"
	@echo "    prune        docker system prune -f"
	@echo "    pull         Pull latest images for all services"
	@echo ""
	@echo "  Data"
	@echo "    fetch-data    Download NAB + SMD into data/ (idempotent)"
	@echo ""
	@echo "  Database"
	@echo "    migrate       Apply feature-store schema (idempotent)"
	@echo ""
	@echo "  Pipeline"
	@echo "    produce-nab   Replay the NAB dataset into events.raw"
	@echo "    produce-smd   Replay the SMD dataset into events.raw"
	@echo "    consume       Run the feature-windowing consumer (Ctrl-C to drain)"
	@echo ""
	@echo "  ML Training"
	@echo "    train-iforest Train the Isolation Forest model"
	@echo ""

# -----------------------------------------------------------------------------
# Stack control
# -----------------------------------------------------------------------------
.PHONY: up
up:
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: volume-down
volume-down:
	$(COMPOSE) down -v --remove-orphans

.PHONY: restart
restart: down up

.PHONY: stop
stop:
	$(COMPOSE) stop

.PHONY: start
start:
	$(COMPOSE) start

# -----------------------------------------------------------------------------
# Observability
# -----------------------------------------------------------------------------
.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: logs-kafka
logs-kafka:
	$(COMPOSE) logs -f kafka-broker-1 kafka-broker-2 kafka-broker-3

.PHONY: logs-pg
logs-pg:
	$(COMPOSE) logs -f postgres

.PHONY: logs-redis
logs-redis:
	$(COMPOSE) logs -f redis

.PHONY: logs-mlflow
logs-mlflow:
	$(COMPOSE) logs -f mlflow

# -----------------------------------------------------------------------------
# Kafka topic automation (host execution)
# -----------------------------------------------------------------------------
# Topics are declared in services/producer/topics.yaml and reconciled by
# topic_admin.py. These targets run on the HOST against the external listener
# ports (19091/19092), the same way the producers will run. I point at the
# venv interpreter directly because `make` does not inherit an activated venv;
# override with `make topics-sync PYTHON=python3` if your setup differs.
# Services run as modules (python -m) so absolute package imports resolve from
# the repo root.
PYTHON      ?= venv/bin/python
TOPIC_ADMIN := services.producer.topic_admin

.PHONY: topics-sync
topics-sync:
	$(PYTHON) -m $(TOPIC_ADMIN) sync

.PHONY: topics-delete
topics-delete:
	$(PYTHON) -m $(TOPIC_ADMIN) delete

.PHONY: topics-list
topics-list:
	$(PYTHON) -m $(TOPIC_ADMIN) list

# -----------------------------------------------------------------------------
# Maintenance
# -----------------------------------------------------------------------------
.PHONY: clean
clean:
	@echo "WARNING: This will delete all volumes and data."
	@read -p "Continue? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v --remove-orphans

.PHONY: prune
prune:
	docker system prune -f

.PHONY: pull
pull:
	$(COMPOSE) pull

# -----------------------------------------------------------------------------
# Dataset acquisition (NAB + SMD)
# -----------------------------------------------------------------------------
# Datasets land in data/ (gitignored). I shallow-clone upstream repos
# (--depth 1: latest tree only, no history) to keep the download small.
#   NAB -> data/NAB                         (CSVs in data/, labels/combined_labels.json)
#   SMD -> data/SMD/ServerMachineDataset    (extracted from the OmniAnomaly repo)
# Paths here match the defaults in services/producer/config.py.
DATA_DIR := data
NAB_REPO := https://github.com/numenta/NAB.git
SMD_REPO := https://github.com/NetManAIOps/OmniAnomaly.git

.PHONY: fetch-data
fetch-data: fetch-nab fetch-smd
	@echo "Datasets ready under $(DATA_DIR)/."

fetch-nab:
	@if [ -d "$(DATA_DIR)/NAB/data" ]; then \
		echo "NAB already present at $(DATA_DIR)/NAB — skipping."; \
	else \
		echo "Cloning NAB -> $(DATA_DIR)/NAB ..."; \
		git clone --depth 1 $(NAB_REPO) $(DATA_DIR)/NAB; \
	fi

fetch-smd:
	@if [ -d "$(DATA_DIR)/SMD/ServerMachineDataset" ]; then \
		echo "SMD already present at $(DATA_DIR)/SMD/ServerMachineDataset — skipping."; \
	else \
		echo "Cloning OmniAnomaly (for SMD) ..."; \
		rm -rf $(DATA_DIR)/_omnianomaly; \
		git clone --depth 1 $(SMD_REPO) $(DATA_DIR)/_omnianomaly; \
		mkdir -p $(DATA_DIR)/SMD; \
		mv $(DATA_DIR)/_omnianomaly/ServerMachineDataset $(DATA_DIR)/SMD/ServerMachineDataset; \
		rm -rf $(DATA_DIR)/_omnianomaly; \
		echo "SMD extracted -> $(DATA_DIR)/SMD/ServerMachineDataset"; \
	fi

# -----------------------------------------------------------------------------
# Producers (host execution, run as modules)
# -----------------------------------------------------------------------------
.PHONY: produce-nab
produce-nab:
	$(PYTHON) -m services.producer.nab_producer

.PHONY: produce-smd
produce-smd:
	$(PYTHON) -m services.producer.smd_producer

# -----------------------------------------------------------------------------
# Consumer (host execution, run as module)
# -----------------------------------------------------------------------------
# Streams events.raw -> rolling-window features -> Postgres (+ Redis cache).
# Long-running: it polls until interrupted, then drains trailing windows and
# commits offsets on a cooperative SIGINT (Ctrl-C). Run `make migrate` first so
# the features table exists.
.PHONY: consume
consume:
	$(PYTHON) -m services.consumer.main

# -----------------------------------------------------------------------------
# Database migrations
# -----------------------------------------------------------------------------
# Apply the consumer's feature-store DDL. The schema file is idempotent
# (CREATE ... IF NOT EXISTS), so re-running is safe. DDL needs the table owner,
# so this runs as the superuser (POSTGRES_USER) - application services connect
# as the least-privilege nsapp user instead. The password is read from .env at
# call time and passed through to the container as PGPASSWORD so psql never
# prompts; it is never written into the Makefile or shell history.
SCHEMA_FILE := services/consumer/schema.sql

.PHONY: migrate
migrate:
	@PGUSER=$$(grep '^POSTGRES_USER=' $(ENV_FILE) | cut -d= -f2-); \
	PGDB=$$(grep '^POSTGRES_DB=' $(ENV_FILE) | cut -d= -f2-); \
	PGPW=$$(grep '^POSTGRES_PASSWORD=' $(ENV_FILE) | cut -d= -f2-); \
	echo "Applying $(SCHEMA_FILE) to $$PGDB as $$PGUSER ..."; \
	$(COMPOSE) exec -T -e PGPASSWORD="$$PGPW" postgres \
		psql -v ON_ERROR_STOP=1 -U "$$PGUSER" -d "$$PGDB" < $(SCHEMA_FILE)


# -----------------------------------------------------------------------------
# ML Training
# -----------------------------------------------------------------------------
.PHONY: train-iforest
train-iforest:
    venv/bin/python -m ml.training.main
