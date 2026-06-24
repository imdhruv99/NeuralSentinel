# NeuralSentinel

Real-time anomaly detection and alerting for high-volume sensor and event streams.

## Infrastructure Overview

The NeuralSentinel infrastructure is modularized using Docker Compose, separating services into distinct compose files linked via a root configuration.

- **Kafka:** Runs in KRaft mode (no ZooKeeper dependency) with a 2-broker data
  cluster plus a dedicated controller node, designed for production-aligned
  fault tolerance. The setup splits internal, external, and controller listeners
  for proper traffic separation. Includes Kafka UI for cluster management.
- **PostgreSQL:** Uses a custom `postgresql.conf` mounted read-only for version-controlled tuning. An initialization script (`init.sql`) automatically creates the `mlflow` database and a least-privilege `nsapp` user. Includes pgAdmin for database administration.
- **Redis:** Configured with persistence (`appendonly yes` and RDB snapshots) to ensure data survives container restarts. Memory is capped at 300MB with an `allkeys-lru` eviction policy, optimized for cache use-cases. Includes a Redis UI.
- **MLflow:** Configured with PostgreSQL as the backend store and a local volume for artifact storage.

---

## Folder Structure

```text
.
├── docs
│   ├── Realtime_Anomaly_Detection.md
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
├── config
│   ├── logging.py                # shared setup_logging() - structured JSON handler
│   └── settings.py               # shared pydantic-settings base classes
├── ml
│   ├── training
│   │   ├── features.py           # shared flatten_feature_map() helper
│   │   ├── isolation_forest_config.py
│   │   ├── isolation_forest_data.py
│   │   ├── isolation_forest_train.py
│   │   ├── lstm_config.py
│   │   ├── lstm_data.py
│   │   ├── lstm_model.py
│   │   ├── lstm_train.py
│   │   └── main.py               # unified entrypoint (iforest | lstm-ae)
│   └── evaluation                # champion-challenger harness
│       ├── config.py             # EvalConfig (PROMOTE_MIN_F1_SCORE, PROMOTE_MIN_DELTA)
│       ├── evaluator.py          # loads model + calibration, scores held-out set → EvalResult
│       ├── promoter.py           # pure decision function (PROMOTE / KEEP / NO_CHAMPION)
│       └── main.py               # orchestrator: find versions → evaluate → transition → audit
├── services
│   ├── common
│   │   └── contracts.py          # EventEnvelope — shared on-the-wire contract
│   ├── producer                  # dataset replay → events.raw
│   │   ├── config.py
│   │   ├── kafka_producer.py
│   │   ├── nab_producer.py
│   │   ├── smd_producer.py
│   │   ├── topic_admin.py
│   │   └── topics.yaml
│   ├── consumer                  # events.raw → windowed features → Postgres/Redis
│   │   ├── config.py
│   │   ├── windowing.py          # pure event-time windowing engine
│   │   ├── sinks.py              # Postgres upsert + Redis cache
│   │   ├── main.py               # poll → window → persist → commit loop
│   │   └── schema.sql            # features + model_promotions DDL
│   ├── scorer                    # (planned) anomaly scoring → events.scored
│   └── alert-api                 # (planned) threshold alerts → alerts
├── requirements.txt
├── Makefile
└── README.md

```

- Note: the `data/` directory (datasets) and `venv/` are created locally and are
  gitignored.

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
| `services/common/contracts.py` | Canonical `EventEnvelope` model — the single source of truth for the on-the-wire message shape, shared by producers and the consumer. |
| `kafka_producer.py` | Builds the `confluent_kafka.Producer` from config and publishes each envelope keyed by `entity_id`. Owns delivery semantics (`acks=all`, idempotence, batching, delivery-report callback). |
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
NAB_STREAM_FILTER=ec2_cpu_utilization_5f5533 venv/bin/python -m services.producer.nab_producer

# Replay one SMD machine (multivariate path)
SMD_MACHINE_FILTER=machine-1-1 venv/bin/python -m services.producer.smd_producer
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
from services.common.contracts import EventEnvelope, Dataset, StreamType
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
from services.producer.config import ProducerConfig
from services.common.contracts import EventEnvelope, Dataset, StreamType
from services.producer.kafka_producer import build_producer, publish, flush_and_close
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

## Feature Windowing Consumer

The consumer is the other half of the ingestion pipeline: it reads raw events
from `events.raw`, groups them per `entity_id`, computes rolling-window features
over **event time**, and persists each window to Postgres (offline training
store) while caching the latest window per entity in Redis (online scoring).

Event time — not wall-clock — is the time axis. The producers replay at max
throughput, so an entire stream arrives in seconds; windows are cut on each
event's own `timestamp`, which makes the feature output identical regardless of
replay speed.

### What is windowing?

A **window** is a fixed slice of time over one stream. Windowing chops a
continuous, never-ending sequence of events into those slices, then reduces each
slice to a single summary row. Instead of "here is event #1,234,567," you get
"here is what this stream looked like between 09:00 and 09:15."

```text
stream of events (one entity, by event-time):
  • • •  • • • • •   • •  • • • •  • •   →  (keeps arriving)
  └── window 1 ──┘└── window 2 ──┘└── window 3 ──┘
        │               │               │
        ▼               ▼               ▼
   one feature row  one feature row  one feature row
   (mean, std,      (mean, std,      (mean, std,
    min, max,        min, max,        min, max,
    slope, count)    slope, count)    slope, count)
```

Each window has two parameters: a **size** (how wide the slice is) and a
**slide** (how far you move before cutting the next one). Everything else —
tumbling vs sliding, the boundary math, the features — builds on those two ideas.

### Why windowing?

A single raw reading - one CPU sample, one latency number - carries almost no
signal on its own. "CPU is 80%" only means something next to what CPU *usually*
is and how fast it's *moving*. Anomalies live in that context, not in the point.
Windowing is how the consumer rebuilds that context from a flat stream.

The job it does is threefold:

- **Bounds an unbounded stream.** Kafka delivers an endless sequence of events;
  models train and score on fixed-size rows. A window collapses "all events for
  this entity between two timestamps" into exactly one feature row — turning a
  firehose into a table.
- **Adds temporal context.** Each window summarizes recent history (mean, spread,
  range, trend) so the detector sees *behavior over time*, not isolated samples.
  This is precisely the context the Isolation Forest and LSTM-AE need.
- **Aligns ragged streams.** NAB streams tick every 5 minutes, SMD every minute;
  different entities start and stop at different points. Cutting every stream on
  the same epoch-aligned boundaries makes their feature rows directly comparable.

**Tumbling vs sliding.** A window is defined by a *size* (how much history it
covers) and a *slide* (how far time advances between rows). When `slide == size`
the windows are **tumbling** — back-to-back, non-overlapping (the current
default: 900s / 900s). When `slide < size` they **slide** — overlapping, which
scores more often and reacts faster at the cost of more rows. Both are the same
engine; only `WINDOW_SIZE_S` and `SLIDE_S` change.

**Boundaries are driven by data, not the clock.** Each entity keeps its own
buffer. As events arrive, their event-time acts as a *watermark*: when it crosses
a window boundary, every window up to that point is emitted and old events are
evicted. A window covers the half-open interval `[end - size, end)`, so no event
is counted twice. On shutdown, the trailing partial window is flushed so nothing
is silently dropped.

**The features per window** (computed in `windowing.py`, no I/O):

| Feature | What it captures |
|---|---|
| `mean` | The window's baseline level. |
| `std` | Volatility / spread — population standard deviation (`0` for a single sample). |
| `min` / `max` | The extremes reached in the window. |
| `slope` | Direction and rate of change `(last - first) / span` — is it trending up or down? |
| `event_count` | How many raw events backed this window (density / confidence). |

For SMD's 38 dimensions these are computed **per metric**, so the feature row is
a map of `{metric: {mean, std, …}}` rather than a flat set of numbers.

### Module layout

| File | Responsibility |
|---|---|
| `config.py` | `ConsumerConfig` (pydantic-settings) — Kafka, window sizing, Postgres DSN, Redis, batching knobs, all overridable via `.env`. |
| `schema.sql` | `features` table DDL. Primary key `(entity_id, window_end)` is what makes writes idempotent. |
| `windowing.py` | Pure, I/O-free windowing engine. Per-entity buffers, epoch-aligned sliding windows, and the mean/std/min/max/slope feature computation. Unit-testable in isolation. |
| `sinks.py` | `FeatureSink` — batched `INSERT ... ON CONFLICT DO NOTHING` into Postgres plus a best-effort Redis cache of the latest window per entity. |
| `main.py` | The poll → window → persist → commit loop. Manual offset commits happen **after** a durable write, and SIGINT drains trailing windows before exit. |

### Delivery semantics

Writes are **persist-before-commit**: a batch is written to Postgres *before* its
Kafka offsets are committed. A crash between the two replays the batch, and the
`(entity_id, window_end)` primary key dedups it via `ON CONFLICT DO NOTHING`. The
result is at-least-once delivery plus idempotent writes — effectively-once
features. The whole Kafka layer (producer, consumer, topic admin) runs on
`confluent-kafka` (librdkafka).

### Window labels

Each window carries a three-state `label`:

| Value | Meaning |
|---|---|
| `true` | The window contains at least one ground-truth anomaly. |
| `false` | The window is known-normal (all events labeled normal). |
| `null` | Unlabeled (e.g. the SMD train split) — deliberately preserved, **not** collapsed to `false`. |

### Running the consumer

The stack must be up, topics created, and the schema applied:

```bash
make migrate        # create the features table (idempotent)
make consume        # start the consumer; Ctrl-C drains and exits cleanly
```

Then drive data through it from another shell:

```bash
make produce-nab    # and/or: make produce-smd
```

Query the resulting feature store:

```sql
SELECT dataset, stream_type, count(*) FROM features GROUP BY 1, 2;
SELECT entity_id, window_start, window_end, event_count, features
FROM features ORDER BY window_end DESC LIMIT 5;
```

The latest window per entity is also cached in Redis under
`features:latest:<entity_id>` as a JSON blob.

---

## ML Training

Both detectors train on windowed features persisted by the consumer.
The stack must be up and features must be loaded before running either job.

### Isolation Forest (`neural-sentinel-isolation-forest-model`)

An `IsolationForest` from scikit-learn is the first-stage detector, optimised
for the NAB UNIVARIATE path. Isolation Forest works by randomly partitioning
the feature space into binary trees; points that are isolated quickly
(short average path length) are anomalous — no notion of "normal" cluster
shape is assumed.

**Architecture choices:**

- Trained on **known-normal rows only** (`label == false`). Anomaly rows are
  excluded from the fit so the model learns only the normal distribution and
  flags deviations from it.
- **Warm-start incremental fitting** (20 checkpoints up to `N_ESTIMATORS=300
  trees`) with per-step progress logs — the forest grows incrementally so you
  can see training progress rather than one blocking call.
- **Time-ordered train/val split** (80/20). Validation rows come from later
  in time so the model is never evaluated on data from its own training window.
- **Threshold from the validation quantile:** `np.quantile(val_scores, CONTAMINATION)`
  on `score_samples` output. Lower score = more anomalous; the threshold is the
  bottom `CONTAMINATION` percentile of validation scores.

**Validation results (NAB · 143,284 windows · 5 features):**

| Metric | Value |
|---|---|
| `valid_roc_auc` | 0.909 |
| `valid_pr_auc` | 0.011 |
| `valid_recall` | 0.381 |
| `threshold (score_samples @ q=0.05)` | −0.390295 |

> PR-AUC is low (~0.011) because the NAB anomaly rate is ~0.5% precision is

> extremely sensitive to the threshold at this imbalance. ROC-AUC of 0.909
> shows the model has strong ranking ability regardless.

```bash
make train-iforest
```

Results are visible in the MLflow UI at http://localhost:58083 under the `neural-sentinel-isolation-forest` experiment. Registered model: `neural-sentinel-isolation-forest-model`.

### LSTM Autoencoder (`neural-sentinel-lstm-autoencoder-model`)

An LSTM-based sequence autoencoder is the second-stage detector, trained on the SMD MULTIVARIATE path (28 machines × 38 metrics). Where the Isolation Forest scores individual feature vectors, the LSTM-AE scores sequences, it learns to reconstruct normal temporal patterns and flags windows where reconstruction error is high.

**Architecture:**

```
Input (batch, T=30, F=190)
    │
    ▼
Encoder LSTM  (n_layers=2, hidden_dim=64)
    │  bottleneck: final hidden + cell state (h_n, c_n)
    ▼
context = h_n[-1] repeated T times  →  (batch, 30, 64)
    │
    ▼
Decoder LSTM  (n_layers=2, hidden_dim=64)
    │
    ▼
Linear projection  →  (batch, 30, 190)  ← reconstruction
    │
    ▼
Anomaly score = mean((input − reconstruction)²) per sequence
```

The decoder receives the same bottleneck vector at every timestep as input - not its own previous output. This prevents the decoder from copying the input step-by-step and forces it to reconstruct purely from the compressed latent representation. A sequence the model has never seen (anomalous) cannot be compressed well, so reconstruction error is high.

**Training choices:**

- Sequences are built per entity with the train/val split applied before windowing - sequences never cross machine boundaries, and no future timestep leaks into training sequences.

- Trained on normal sequences only (same philosophy as IForest).

- Early stopping (`patience=5`) with best-checkpoint restore the weights with lowest validation loss are kept, not the final epoch's weights.

- Threshold: `np.quantile(val_errors, 1 − CONTAMINATION)` the top
`CONTAMINATION` fraction of validation reconstruction errors. Note the direction is opposite to IForest: high error = anomalous.

- Model exported as TorchScript (`torch.jit.trace`) before registration. The scoring consumer can `torch.jit.load()` it without importing the model class definition.

**Validation results (SMD · 36,976 train sequences · 8,644 val sequences · 190 features)**:

| Metric | Value |
|---|---|
| `val_best_loss`(MSE) | 0.6716 |
| `threshold (recon_error @ q=0.95)` | 2.755 |
| `val_predicted_anomaly_rate` | 0.050 |
| Epochs trained | 28 / 50 (early stop) |

```
make train-lstm-ae
```

Results are visible in the MLflow UI at http://localhost:58083 under the `neural-sentinel-lstm-autoencoder` experiment. Registered model: `neural-sentinel-lstm-autoencoder-model`.

---

## Champion–Challenger Evaluation & Model Promotion

After training, a dedicated evaluation harness compares the **challenger** (newest version) against the current **champion** (Production stage) on a held-out labeled validation set. If the challenger beats the champion by a configured margin it is automatically transitioned to Production in the MLflow registry and the old version is archived.

### How it works

```text
Challenger (latest version)           Champion (Production version)
        │                                         │
        ▼                                         ▼
  load model + calibration               load model + calibration
        │                                         │
        ▼                                         ▼
  score held-out windows                score held-out windows
  precision / recall / F1               precision / recall / F1
  PR-AUC / ROC-AUC                      PR-AUC / ROC-AUC
        │                                         │
        └──────────────┬───────────────────────── ┘
                       ▼
              promoter.decide()
               PROMOTE / KEEP / NO_CHAMPION
                       │
          ┌────────────┴─────────────────┐
          ▼                              ▼
  transition challenger         keep current champion
  → Production                  in Production
  archive champion
          │
          ▼
  INSERT audit row → model_promotions (Postgres)
```

**Decision rules** (configured in `.env`):

| Condition | Decision |
|---|---|
| No champion exists yet | `NO_CHAMPION` → promote unconditionally |
| `challenger.f1 < PROMOTE_MIN_F1_SCORE` | `KEEP` — challenger is too weak regardless of delta |
| `challenger.f1 − champion.f1 ≥ PROMOTE_MIN_DELTA` | `PROMOTE` |
| Otherwise | `KEEP` |

### Module layout

| File | Responsibility |
|---|---|
| `ml/evaluation/config.py` | `EvalConfig` — inherits Postgres + MLflow settings; exposes `PROMOTE_MIN_F1_SCORE`, `PROMOTE_MIN_DELTA`, `EVAL_VALIDATION_RATIO`. |
| `ml/evaluation/evaluator.py` | Loads model + calibration artifact from MLflow, queries windowed features from Postgres, returns `EvalResult(precision, recall, f1, pr_auc, roc_auc, n_samples, anomaly_rate)`. |
| `ml/evaluation/promoter.py` | Pure, I/O-free decision function. `decide(challenger, champion, min_f1, min_delta) → PromotionVerdict`. |
| `ml/evaluation/main.py` | Orchestrator: find Staging/latest version → evaluate → call promoter → MLflow stage transition → write audit log. |

### Audit log

Every evaluation writes one row to the `model_promotions` table in the `projects` Postgres database:

```sql
SELECT model_name, challenger_version, champion_version, decision, reason, promoted_at
FROM model_promotions ORDER BY id;
```

### Running evaluation

```bash
make evaluate-iforest   # evaluate neural-sentinel-isolation-forest-model
make evaluate-lstm      # evaluate neural-sentinel-lstm-autoencoder-model
```

First run (no Production version) → `NO_CHAMPION` → challenger is promoted to Production automatically.
Subsequent runs compare the latest version against the incumbent.

### Config knobs

| Variable | Default | Meaning |
|---|---|---|
| `PROMOTE_MIN_F1_SCORE` | `0.0` | Minimum absolute F1 the challenger must reach |
| `PROMOTE_MIN_DELTA` | `0.01` | Minimum F1 improvement over champion to trigger promotion |
| `EVAL_VALIDATION_RATIO` | `0.2` | Fraction of the feature store used as the held-out eval set |
