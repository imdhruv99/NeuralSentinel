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
PYTHON      ?= venv/bin/python
TOPIC_ADMIN := services/producer/topic_admin.py

.PHONY: topics-sync
topics-sync:
	$(PYTHON) $(TOPIC_ADMIN) sync

.PHONY: topics-delete
topics-delete:
	$(PYTHON) $(TOPIC_ADMIN) delete

.PHONY: topics-list
topics-list:
	$(PYTHON) $(TOPIC_ADMIN) list

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
