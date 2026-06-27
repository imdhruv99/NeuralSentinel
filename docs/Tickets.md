# Project 01 — Tickets: Real-Time Anomaly Detection Platform

> 15 Jira-style tickets · Build order 1 (Phase 1). Tech: Kafka, IsolationForest + LSTM-AE (PyTorch/sklearn), FastAPI, React, MLflow, Dagster, Postgres/Redis, Docker Compose.
> Epics: A Infra · B Ingestion · C Modeling · D Serving · E Frontend · F MLOps · G Quality.

---

### RTAD-001 — Bootstrap repo + Docker Compose skeleton - COMPLETED

- **Type:** Task · **Epic:** A Infra · **Priority:** Critical · **Points:** 3
- **Description:** Create the mono-repo layout and a Compose stack that brings up Kafka, Zookeeper/KRaft, Postgres, Redis, and MLflow so all later work has a runnable base.
- **Acceptance Criteria:**
  - `docker compose up` starts Kafka, Postgres, Redis, MLflow with healthchecks green.
  - `make up` / `make down` helpers documented in README.
  - `.env.example` enumerates all required variables.
- **Technical Steps:** 1) Init repo, `pyproject.toml`, ruff/black/mypy. 2) Author `docker-compose.yml` (Kafka KRaft, Postgres 16, Redis 7, MLflow + artifact store). 3) Add healthchecks + named volumes. 4) Wire `Makefile`.
- **Dependencies:** none
- **Config:** `KAFKA_BOOTSTRAP=localhost:9092`, `POSTGRES_*`, `REDIS_URL`, `MLFLOW_TRACKING_URI`.
- **DoD:** Stack boots clean on a fresh machine; README "Setup" section complete.

### RTAD-002 — Define event schema + declarative Kafka topic automation - COMPLETED

- **Type:** Task · **Epic:** A · **Priority:** High · **Points:** 2
- **Description:** Establish a single canonical `EventEnvelope` that wraps **both** dataset shapes (NAB univariate + SMD 38-dim multivariate) and manage Kafka topics declaratively, so every producer/consumer shares one contract. Schema and topics are co-designed because the envelope's `metrics` payload differs per dataset — this is why RTAD-002 and RTAD-003 are interconnected.
- **Acceptance Criteria:** Versioned JSON `EventEnvelope` (with `dataset`, `stream_type`, `metrics{}`, ground-truth `is_anomaly`) documented in `/docs/schema.md`; topics `events.raw`, `events.scored`, `alerts` declared in `topics.yaml` and reconciled by an idempotent `topic_admin.py` (create / update-config / delete / list); partition + replication rationale documented (RF=2 for a 2-broker cluster; partition key = `entity_id`).
- **Technical Steps:** 1) Define `EventEnvelope` (event_id, entity_id, dataset, stream_type, timestamp, ingest_ts, metrics{}, is_anomaly, sequence_idx). 2) Declare topics in `topics.yaml`. 3) Idempotent `topic_admin.py sync/delete/list` against the cluster. 4) Document partitioning by `entity_id` (ordering invariant required by the LSTM-AE sequence buffer). 5) Wire `make topics-sync`.
- **Dependencies:** RTAD-001
- **Config:** `KAFKA_BOOTSTRAP_SERVERS`, `EVENTS_TOPIC`, `SCORED_TOPIC`, `ALERTS_TOPIC`; partitions 6/6/3, RF=2.
- **DoD:** `make topics-sync` creates/updates topics idempotently; `EventEnvelope` schema doc lives in `/docs`; partition-key strategy documented.

### RTAD-003 — Dataset replay producers (NAB + SMD) - COMPLETED

- **Type:** Story · **Epic:** B Ingestion · **Priority:** High · **Points:** 3
- **Description:** Replay two **real, labeled** anomaly datasets into `events.raw` as if they were live telemetry, instead of generating synthetic data. NAB (58 univariate streams) feeds the Isolation Forest path; SMD (28 machines × 38 metrics) feeds the LSTM-AE path. Each row becomes an `EventEnvelope` keyed by `entity_id`, preserving the dataset's ground-truth anomaly labels for downstream evaluation.
- **Acceptance Criteria:** `nab_producer.py` and `smd_producer.py` replay their datasets to `events.raw`; ground-truth labels mapped onto `is_anomaly`; configurable replay speed (0 = max throughput, 1.0 = wall-clock real time); per-stream / per-machine filter for fast local runs; deterministic per-`entity_id` ordering; one-command data acquisition into a gitignored `data/`.
- **Technical Steps:** 1) Data acquisition tooling (clone NAB; fetch SMD `ServerMachineDataset`) into `data/`. 2) NAB CSV reader → EventEnvelope (univariate, label from `combined_labels.json`). 3) SMD reader → EventEnvelope (38-dim, label from `test_label/`). 4) Shared serialization + `entity_id`-keyed Kafka publish. 5) Replay-speed + filter via env/CLI. 6) Dockerize as a service.
- **Dependencies:** RTAD-002
- **Config:** `NAB_DATA_DIR`, `NAB_LABELS_FILE`, `NAB_REPLAY_SPEED`, `NAB_STREAM_FILTER`, `SMD_DATA_DIR`, `SMD_REPLAY_SPEED`, `SMD_MACHINE_FILTER`, `SMD_USE_TEST_SPLIT`.
- **DoD:** Both datasets stream into `events.raw` with labels intact; replay speed + filters work; `data/` is gitignored; consumer (RTAD-004) observes ordered per-entity events.

### RTAD-004 — Streaming ingestion + feature windowing consumer - COMPLETED

- **Type:** Story · **Epic:** B · **Priority:** High · **Points:** 5
- **Description:** Consume `events.raw`, compute rolling-window features per entity, and persist to Postgres for training and to Redis for online scoring.
- **Acceptance Criteria:** Tumbling + sliding window features (mean/std/min/max/rate); idempotent writes; consumer-group offset commits after persist.
- **Technical Steps:** 1) Kafka consumer w/ manual commit. 2) Windowed feature computation. 3) Redis online cache, Postgres offline write. 4) Backpressure handling.
- **Dependencies:** RTAD-002, RTAD-003
- **Config:** `WINDOW_SIZE_S`, `SLIDE_S`, `CONSUMER_GROUP`.
- **DoD:** Features queryable in Postgres; latency < 1s p95 from event to feature.

### RTAD-005 — Isolation Forest baseline model + training job - COMPLETED

- **Type:** Story · **Epic:** C Modeling · **Priority:** High · **Points:** 5
- **Description:** Train an Isolation Forest on historical windowed features as the first detector and log it to MLflow.
- **Acceptance Criteria:** Training reads Postgres features; logs params/metrics/model to MLflow; reproducible via seed.
- **Technical Steps:** 1) Feature loader. 2) sklearn IsolationForest pipeline w/ scaler. 3) MLflow autolog + signature. 4) Persist threshold from validation.
- **Dependencies:** RTAD-004
- **Config:** `CONTAMINATION`, `N_ESTIMATORS`, `MLFLOW_EXPERIMENT`.
- **DoD:** Model registered in MLflow registry as `anomaly-iforest`; eval metrics recorded.

### RTAD-006 — LSTM Autoencoder detector (PyTorch) - COMPLETED

- **Type:** Story · **Epic:** C · **Priority:** High · **Points:** 8
- **Description:** Implement a sequence LSTM-AE that flags anomalies by reconstruction error, complementing the tree-based detector for temporal patterns.
- **Acceptance Criteria:** Trains on sequences; reconstruction-error threshold from validation quantile; GPU-optional, CPU-runnable; logged to MLflow.
- **Technical Steps:** 1) Sequence windowing dataset. 2) LSTM-AE module + train loop w/ early stopping. 3) Threshold calibration. 4) MLflow logging + TorchScript export.
- **Dependencies:** RTAD-004
- **Config:** `SEQ_LEN`, `HIDDEN_DIM`, `EPOCHS`, `LR`.
- **DoD:** `anomaly-lstmae` registered; ROC-AUC reported on the SMD labeled test split.

### RTAD-007 — Champion–Challenger evaluation + promotion - COMPLETED

- **Type:** Story · **Epic:** F MLOps · **Priority:** High · **Points:** 5
- **Description:** Evaluate both detectors on a held-out labeled set and auto-promote the winner to the `Production` stage in MLflow.
- **Acceptance Criteria:** Shared eval harness (precision/recall/F1/PR-AUC); promotion gated on threshold + improvement over current champion; decision logged.
- **Technical Steps:** 1) Eval harness over labeled anomalies. 2) Compare vs current Production model. 3) MLflow stage transition API. 4) Audit record in Postgres.
- **Dependencies:** RTAD-005, RTAD-006
- **Config:** `PROMOTE_MIN_F1`, `PROMOTE_MIN_DELTA`.
- **DoD:** Re-running with a better challenger flips Production automatically.

### RTAD-008 — Real-time scoring consumer - COMPLETED

- **Type:** Story · **Epic:** D Serving · **Priority:** Critical · **Points:** 5
- **Description:** Load the current Production model, score windowed features in real time, and emit results to `events.scored` and threshold-crossing `alerts`.
- **Acceptance Criteria:** Hot-reload on model promotion (no restart); scores within window SLA; alerts deduped per entity within cooldown.
- **Technical Steps:** 1) Model loader from MLflow registry. 2) Scoring consumer. 3) Alert rule + cooldown. 4) Emit to topics + Postgres.
- **Dependencies:** RTAD-004, RTAD-007
- **Config:** `ALERT_COOLDOWN_S`, `MODEL_STAGE=Production`.
- **DoD:** End-to-end: a labeled (NAB/SMD) anomaly → alert in < 2s p95.

### RTAD-009 — Alert/Query API (FastAPI) - COMPLETED

- **Type:** Story · **Epic:** D · **Priority:** High · **Points:** 3
- **Description:** Expose REST + SSE endpoints for live alerts, entity history, and current model metadata.
- **Acceptance Criteria:** `/alerts` (paginated), `/entities/{id}/series`, `/model/current`, `/healthz`; SSE stream `/alerts/stream`; OpenAPI docs.
- **Technical Steps:** 1) FastAPI app + pydantic models. 2) Postgres queries. 3) SSE generator from Redis pub/sub. 4) Auth via API key.
- **Dependencies:** RTAD-008
- **Config:** `API_KEY`, `PORT=8000`.
- **DoD:** Swagger UI live; SSE pushes alerts in real time.

### RTAD-010 — React dashboard

- **Type:** Story · **Epic:** E Frontend · **Priority:** High · **Points:** 5
- **Description:** A functional dashboard showing live time-series, anomaly markers, an alert feed, and the active model.
- **Acceptance Criteria:** Live charts (Recharts/D3); anomalies highlighted; SSE-driven alert feed; model banner; responsive.
- **Technical Steps:** 1) Vite + React + TS. 2) Series + anomaly chart. 3) SSE alert feed component. 4) Model/status header.
- **Dependencies:** RTAD-009
- **Config:** `VITE_API_BASE`.
- **DoD:** Demo-able UI; screenshot in README.

### RTAD-011 — Dagster retraining pipeline

- **Type:** Story · **Epic:** F · **Priority:** Medium · **Points:** 5
- **Description:** Orchestrate scheduled retraining: extract features → train both models → Champion–Challenger → promote, as Dagster assets.
- **Acceptance Criteria:** Asset graph runs end-to-end; schedule + manual trigger; run history visible in Dagit.
- **Technical Steps:** 1) Define assets wrapping 004–007. 2) Schedule (e.g., daily). 3) Sensors for data volume. 4) Failure alerting.
- **Dependencies:** RTAD-005, RTAD-006, RTAD-007
- **Config:** `RETRAIN_CRON`, `MIN_NEW_ROWS`.
- **DoD:** A scheduled run retrains and can promote a new champion unattended.

### RTAD-012 — Observability (Prometheus + Grafana)

- **Type:** Task · **Epic:** G Quality · **Priority:** Medium · **Points:** 3
- **Description:** Instrument consumers/API with metrics (throughput, lag, scoring latency, alert rate) and ship a Grafana dashboard.
- **Acceptance Criteria:** `/metrics` endpoints; consumer lag tracked; Grafana dashboard JSON committed.
- **Technical Steps:** 1) prometheus-client counters/histograms. 2) Compose Prometheus + Grafana. 3) Dashboard + provisioning.
- **Dependencies:** RTAD-008, RTAD-009
- **Config:** `PROM_PORT`.
- **DoD:** Dashboard shows live throughput, lag, latency, alerts.

### RTAD-013 — Test suite (unit + integration + e2e)

- **Type:** Task · **Epic:** G · **Priority:** High · **Points:** 5
- **Description:** Cover feature windowing, model thresholds, promotion logic, and a full inject→alert e2e using Testcontainers.
- **Acceptance Criteria:** ≥80% coverage on core libs; e2e asserts a labeled (NAB/SMD) anomaly produces an alert; CI runs green.
- **Technical Steps:** 1) pytest unit tests. 2) Testcontainers Kafka/Postgres integration. 3) e2e harness. 4) GitHub Actions CI.
- **Dependencies:** RTAD-008, RTAD-011
- **Config:** `CI=true`.
- **DoD:** CI badge green; coverage report published.

### RTAD-014 — Data quality + drift guard

- **Type:** Story · **Epic:** G · **Priority:** Medium · **Points:** 3
- **Description:** Validate incoming feature distributions and flag drift that should trigger retraining.
- **Acceptance Criteria:** Schema + range checks on features; PSI/KS drift metric logged; drift breach raises a Dagster sensor event.
- **Technical Steps:** 1) Validation step in consumer. 2) Drift metric vs training baseline. 3) Hook to retraining sensor.
- **Dependencies:** RTAD-004, RTAD-011
- **Config:** `DRIFT_PSI_THRESHOLD`.
- **DoD:** Synthetic drift triggers a retraining run.

### RTAD-015 — Documentation + architecture README

- **Type:** Task · **Epic:** G · **Priority:** High · **Points:** 2
- **Description:** Author the portfolio-grade README: Problem → Architecture → Setup → Usage → Design Decisions → Future Work, with the Mermaid diagram.
- **Acceptance Criteria:** All six sections present; diagram renders; one-command demo path documented; design trade-offs explained.
- **Technical Steps:** 1) Write README. 2) Add Mermaid + screenshots. 3) Record a short demo gif. 4) Document design decisions.
- **Dependencies:** all
- **Config:** n/a
- **DoD:** A reviewer can clone, run, and understand the system in < 15 min.

---

**Suggested order:** 001→002→003→004→005→006→007→008→009→010→011→012→013→014→015.
**Critical path:** 001→004→ (005,006) →007→008→009→010.
