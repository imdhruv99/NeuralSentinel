# Event Schema — NeuralSentinel

> RTAD-002 deliverable: schema documentation for `events.raw`, `events.scored`, `alerts`.

---

## Why a unified envelope?

NeuralSentinel ingests data from two structurally different datasets:

| | NAB | SMD |
|---|---|---|
| Type | Univariate | Multivariate |
| Metrics | 1 (value) | 38 (feature_0..feature_37) |
| Labels | Timestamp list | Per-row binary file |
| Anomaly type | Point / contextual | Temporal / collective |
| Model | Isolation Forest | LSTM Autoencoder |

Rather than creating separate topics per dataset (which would require separate consumer branches and API query paths), we define a single `EventEnvelope` that wraps both. The `dataset` and `stream_type` fields tell downstream services how to interpret the `metrics` dict.

---

## EventEnvelope

Canonical Kafka message value for the `events.raw` and `events.scored` topics.

```json
{
  "event_id":     "550e8400-e29b-41d4-a716-446655440000",
  "entity_id":    "NAB/realAWSCloudwatch/cpu_24hr_activate",
  "dataset":      "NAB",
  "stream_type":  "UNIVARIATE",
  "timestamp":    "2014-04-11 00:00:00",
  "ingest_ts":    "2025-06-01T10:32:01.423Z",
  "metrics":      {"value": 9.6},
  "is_anomaly":   null,
  "sequence_idx": 0
}
```

### Field reference

| Field | Type | Required | Description |
|---|---|---|---|
| `event_id` | string (UUID4) | ✓ | Unique per event. Used for dedup in idempotent consumers. |
| `entity_id` | string | ✓ | Identifies the stream. Used as Kafka partition key. Format: `"<DATASET>/<stream_name>"`. |
| `dataset` | enum | ✓ | `"NAB"` \| `"SMD"` \| `"SYNTHETIC"`. Drives model selection. |
| `stream_type` | enum | ✓ | `"UNIVARIATE"` \| `"MULTIVARIATE"`. Tells consumer how to window features. |
| `timestamp` | string (ISO-8601) | ✓ | Original measurement time from the dataset. **Not** ingestion wall clock. |
| `ingest_ts` | string (ISO-8601) | ✓ | UTC wall clock when producer published. Use for ingestion lag monitoring. |
| `metrics` | object | ✓ | Measurement payload. See per-dataset structure below. |
| `is_anomaly` | bool \| null | | Ground truth label from dataset. `null` = no label available (production / train split). |
| `sequence_idx` | int | | Zero-based row index within the stream. Used by LSTM-AE to detect gaps in the sequence buffer. |

### metrics — NAB (UNIVARIATE)

```json
{"value": 9.6}
```

Single key: `value`. Float. The raw sensor or metric reading from the NAB CSV.

### metrics — SMD (MULTIVARIATE)

```json
{
  "feature_0":  0.12,
  "feature_1":  0.88,
  "feature_2":  0.04,
  ...
  "feature_37": 0.71
}
```

38 keys: `feature_0` through `feature_37`. Floats. The OmniAnomaly paper provides
partial descriptions of a subset of features for the server machines (CPU load,
memory read/write rates, network I/O, etc.), but the dataset doesn't include a
machine-readable column name mapping — positional names are used throughout.

---

## Kafka Topics

### `events.raw`

Raw events from producers. One message per dataset row.

| Property | Value | Rationale |
|---|---|---|
| Partitions | 6 | Headroom for NAB (58 streams) + SMD (28 machines). Key = `entity_id`. |
| Replication factor | 2 | Matches broker count. RF=3 would fail with 2 brokers. |
| Retention | 72 hours | Covers replay + downstream processing buffer. |
| Cleanup policy | delete | We need full history for LSTM windowing, not compaction. |

### `events.scored`

Scored events emitted by the Scorer service. Schema extends `EventEnvelope` with scoring fields (defined in the scorer service — RTAD-008).

| Property | Value |
|---|---|
| Partitions | 6 |
| Replication factor | 2 |
| Retention | 24 hours |

The scorer uses the same `entity_id` partition key, preserving the ordering invariant from `events.raw`. A scored event can be correlated back to its raw event by `event_id`.

### `alerts`

Threshold-crossing anomaly alerts. Low volume — only emitted when the scorer's anomaly score exceeds the configured threshold.

| Property | Value |
|---|---|
| Partitions | 3 |
| Replication factor | 2 |
| Retention | 24 hours |
| Max message size | 64 KB (alerts are small) |

Alert schema is defined in the Alert API service (RTAD-009).

---

## Partition key strategy

```
Kafka partition = hash(entity_id) % num_partitions
```

All events for one stream (e.g. `"NAB/realAWSCloudwatch/cpu_24hr_activate"`) land on the same partition, guaranteeing arrival order within that stream. This is a **hard requirement** for the LSTM Autoencoder:

The LSTM-AE scorer buffers a sliding window of `SEQ_LEN` consecutive events before scoring. If events from one stream arrived on different partitions (and therefore different ordering contexts), the window would mix events from different time positions — the model would receive corrupted sequences and produce meaningless reconstruction errors.

---

## Data flow summary

```
NAB CSVs (58 streams, univariate)   ─┐
                                      ├─► nab_producer.py ─► events.raw (partitioned by entity_id)
SMD TXTs (28 machines, 38-dim)      ─┘                            │
                                                                   ▼
                                                         consumer/windowing
                                                         (RTAD-004: feature extraction)
                                                                   │
                                                      ┌────────────┴────────────┐
                                                      ▼                         ▼
                                              IForest (NAB)          LSTM-AE (SMD)
                                              RTAD-005               RTAD-006
                                                      └────────────┬────────────┘
                                                                   ▼
                                                            events.scored
                                                                   │
                                                                   ▼
                                                     alert rule + cooldown (RTAD-008)
                                                                   │
                                                                   ▼
                                                                alerts
```
