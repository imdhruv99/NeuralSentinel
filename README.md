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

## Data Pipeline: Dataset Replay Producers

Ingestion replays two real, labeled anomaly datasets into `events.raw` as if they
were live telemetry, no synthetic generation. Each dataset row becomes a single
`EventEnvelope`, keyed by `entity_id`, and is published to Kafka. Ground-truth
labels are preserved on the `is_anomaly` field for downstream evaluation.

| Dataset | Shape | Stream type | Detector path | Label source |
|---|---|---|---|---|
| **NAB** (Numenta Anomaly Benchmark) | Univariate (`value`) | `UNIVARIATE` | Isolation Forest | `combined_labels.json` (anomaly timestamps) |
| **SMD** (Server Machine Dataset) | 38-dim (`feature_0..37`) | `MULTIVARIATE` | LSTM Autoencoder | `test_label/` (row-aligned 0/1) |

### Module layout

| File | Responsibility |
|---|---|
| `envelope.py` | Canonical `EventEnvelope` model — the single source of truth for the on-the-wire message shape, shared by producers and (later) consumers. |
| `kafka_producer.py` | Builds the `KafkaProducer` from config and publishes each envelope keyed by `entity_id`. Owns delivery semantics (`acks=all`, idempotence, batching). |
| `nab_producer.py` | Walks NAB CSVs row-by-row, maps anomaly timestamps onto `is_anomaly`, and replays each stream. |
| `smd_producer.py` | Walks SMD machine files, parses 38-dim rows, attaches row-aligned test labels, and synthesizes a monotonic timestamp per row. |

Keying every message by `entity_id` is what guarantees per-stream ordering — a
hard requirement for the LSTM-AE sequence buffer. All events for one stream land
on the same partition and are therefore consumed in order.

### Acquiring the datasets

Both datasets are distributed as git repositories and are shallow-cloned into the
gitignored `data/` directory. The target is idempotent and skips anything already
present:

```bash
make fetch-data
```

Resulting layout (matches the defaults in `config.py`):

```text
data/
├── NAB/data/<category>/<stream>.csv        # e.g. realAWSCloudwatch/ec2_cpu_utilization_5f5533.csv
├── NAB/labels/combined_labels.json
└── SMD/ServerMachineDataset/{train,test,test_label}/<machine>.txt
```

> NAB is licensed AGPL-3.0 and SMD is MIT; neither is redistributed in this
> repository. The `data/` directory is gitignored and populated locally.

### Running the producers

The stack must be up (`make up`) and the topics created (`make topics-sync`).
Producers run on the host against the external listener ports. Each accepts a
filter env var to replay a single stream/machine for fast local runs:

```bash
# Replay one NAB stream (univariate path)
NAB_STREAM_FILTER=ec2_cpu_utilization_5f5533 venv/bin/python services/producer/nab_producer.py

# Replay one SMD machine (multivariate path)
SMD_MACHINE_FILTER=machine-1-1 venv/bin/python services/producer/smd_producer.py
```

Replay speed is controlled by `NAB_REPLAY_SPEED` / `SMD_REPLAY_SPEED`
(`0.0` = max throughput; larger values throttle each row). Omitting the filter
replays all 58 NAB streams / 28 SMD machines. The SMD split is selectable via
`SMD_USE_TEST_SPLIT` (test split carries labels; train split does not).

### Smoke tests

The envelope contract and the Kafka path can be verified without any dataset on
disk.

Envelope serialization (no broker needed):

```bash
python -c "
import sys; sys.path.insert(0, 'services/producer')
from envelope import EventEnvelope, Dataset, StreamType
e = EventEnvelope(
    entity_id='NAB/smoke-test', dataset=Dataset.NAB,
    stream_type=StreamType.UNIVARIATE, timestamp='2014-04-11 00:00:00',
    metrics={'value': 9.6}, is_anomaly=False, sequence_idx=0,
)
print(e.to_json_bytes().decode())
"
```

Expected: one JSON line containing a random `event_id`, a current-UTC
`ingest_ts`, `"dataset":"NAB"`, and `"metrics":{"value":9.6}`.

End-to-end publish (stack must be up):

```bash
python -c "
import sys; sys.path.insert(0, 'services/producer')
from config import ProducerConfig
from envelope import EventEnvelope, Dataset, StreamType
from kafka_producer import build_producer, publish, flush_and_close
cfg = ProducerConfig()
p = build_producer(cfg)
ev = EventEnvelope(
    entity_id='NAB/smoke-test', dataset=Dataset.NAB,
    stream_type=StreamType.UNIVARIATE, timestamp='2014-04-11 00:00:00',
    metrics={'value': 1.23},
)
publish(p, cfg.topic_events_raw, ev)
flush_and_close(p)
print('published + flushed OK')
"
```

Expected: `published + flushed OK`. The message is visible in Kafka UI
(http://localhost:18080) under the `events.raw` topic.

---
