# NeuralSentinel

Real-time anomaly detection and alerting for high-volume sensor and event streams.

## Infrastructure Overview

The NeuralSentinel infrastructure is modularized using Docker Compose, separating services into distinct compose files linked via a root configuration.

- **Kafka:** Runs in KRaft mode (no ZooKeeper dependency) with a 3-broker cluster, designed for production-aligned fault tolerance. The setup splits internal, external, and controller listeners for proper traffic separation. Includes Kafka UI for cluster management.
- **PostgreSQL:** Uses a custom `postgresql.conf` mounted read-only for version-controlled tuning. An initialization script (`init.sql`) automatically creates the `mlflow` database and a least-privilege `nsapp` user. Includes pgAdmin for database administration.
- **Redis:** Configured with persistence (`appendonly yes` and RDB snapshots) to ensure data survives container restarts. Memory is capped at 300MB with an `allkeys-lru` eviction policy, optimized for cache use-cases. Includes a Redis UI.
- **MLflow:** Configured with PostgreSQL as the backend store and a local volume for artifact storage.

---

## Folder Structure

```text
.
├── docs
│   ├── schema.md
│   └── Tickets.md
├── infra
│   ├── conf
│   │   ├── init.sql
│   │   ├── pg_hba.conf
│   │   ├── postgresql.conf
│   │   └── redis.conf
│   ├── docker-compose.yaml
│   ├── kafka-docker-compose.yaml
│   ├── mlflow-docker-compose.yaml
│   ├── postgres-docker-compose.yaml
│   └── redis-docker-compose.yaml
├── services
│   └── producer
│       ├── config.py
│       ├── topic_admin.py
│       └── topics.yaml
├── Makefile
└── README.md

```

- Note: data directory will be created after running the compose.

---

## Getting Started: Starting the Infrastructure

The project utilizes a `Makefile` to simplify infrastructure management. Ensure you have Docker and Docker Compose installed.

### 1. Environment Setup

Before starting, ensure an `.env` file is present in the root directory. Do not commit actual secrets to version control; use an `.env.example` for scaffolding.

### 2. Stack Control

Run the following Make commands from the root directory to manage the stack:

- **Start the infrastructure:** `make up` (Starts all services in detached mode).
- **Stop and remove containers:** `make down`.
- **Stop containers without removing them:** `make stop`.
- **Start existing containers:** `make start`.
- **Restart the stack:** `make restart`.

---

## Kafka Topics & Event Schema

All events share a single canonical `EventEnvelope` that wraps both dataset
shapes — NAB (univariate) and SMD (38-dim multivariate). The full field
reference, per-dataset `metrics` structure, and partition-key strategy live in
[docs/schema.md](docs/schema.md).

Topics are managed **declaratively**: the desired state lives in
[services/producer/topics.yaml](services/producer/topics.yaml), and
`topic_admin.py` reconciles the live cluster against it (create missing topics,
update configs on existing ones, never silently change partition count).

| Topic | Partitions | RF | Retention | Purpose |
|---|---|---|---|---|
| `events.raw` | 6 | 2 | 72h | Raw dataset rows replayed by the producers |
| `events.scored` | 6 | 2 | 24h | Events annotated with anomaly scores |
| `alerts` | 3 | 2 | 24h | Threshold-crossing anomaly alerts |

> Replication factor is **2** because the cluster runs **2 brokers**. The
> partition key is `entity_id`, which guarantees per-stream ordering — a hard
> requirement for the LSTM-AE sequence buffer.

Manage topics from the host (uses the external listener ports):

- **Create / update topics:** `make topics-sync` (idempotent)
- **List cluster topics:** `make topics-list`
- **Delete all declared topics:** `make topics-delete` (destructive; needed to
  change partition count or replication factor, which require delete + recreate)

---
