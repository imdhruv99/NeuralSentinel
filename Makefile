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
	@echo "    topics       List Kafka topics"
	@echo "    topic-create Create default NeuralSentinel topics"
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
# Kafka helpers
# -----------------------------------------------------------------------------
KAFKA_BROKER  := kafka-broker-1
KAFKA_INTERNAL := kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

.PHONY: topics
topics:
	$(COMPOSE) exec $(KAFKA_BROKER) \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(KAFKA_INTERNAL) --list

.PHONY: topic-create
topic-create:
	$(COMPOSE) exec $(KAFKA_BROKER) \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(KAFKA_INTERNAL) \
		--create --if-not-exists --topic events.raw      --partitions 6 --replication-factor 3
	$(COMPOSE) exec $(KAFKA_BROKER) \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(KAFKA_INTERNAL) \
		--create --if-not-exists --topic events.scored   --partitions 6 --replication-factor 3
	$(COMPOSE) exec $(KAFKA_BROKER) \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server $(KAFKA_INTERNAL) \
		--create --if-not-exists --topic alerts          --partitions 3 --replication-factor 3
	@echo "Topics created."

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
