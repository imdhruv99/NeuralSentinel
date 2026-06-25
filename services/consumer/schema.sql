-- Offline feature store written by the windowing consumer
-- Applied once at setup.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS features (
    entity_id     TEXT        NOT NULL,
    dataset       TEXT        NOT NULL,
    stream_type   TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    event_count   INTEGER     NOT NULL,
    features      JSONB       NOT NULL,
    label         BOOLEAN,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- window_end is deterministic from event-time, so reprocessing after a
    -- consumer rebalance produces the SAME key -> upsert is idempotent.
    PRIMARY KEY (entity_id, window_end)
);

CREATE INDEX IF NOT EXISTS idx_features_dataset_time
    ON features (dataset, window_end);


CREATE TABLE IF NOT EXISTS model_promotions (
    id                SERIAL PRIMARY KEY,
    model_name        TEXT        NOT NULL,
    challenger_version INTEGER    NOT NULL,
    champion_version  INTEGER,
    decision          TEXT        NOT NULL,
    reason            TEXT        NOT NULL,
    challenger_metrics JSONB      NOT NULL,
    champion_metrics  JSONB,
    promoted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_promotions_model_time
    ON model_promotions (model_name, promoted_at DESC);


-- ---------------------------------------------------------------------------
-- Online anomaly score store
--
-- Written by the real-time scoring service after every scored feature window.
-- Primary key (entity_id, window_end, model_name) is intentionally composite:
-- multiple detector models can score the same window independently, and
-- idempotent re-scoring on restart is a no-op (ON CONFLICT DO NOTHING).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS scores (
    entity_id     TEXT        NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    model_name    TEXT        NOT NULL,
    model_version INTEGER     NOT NULL,
    anomaly_score FLOAT8      NOT NULL,
    is_anomaly    BOOLEAN     NOT NULL,
    scored_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (entity_id, window_end, model_name)
);

-- Supports the most common query pattern: "give me all anomalies for model X
-- after time T", used by the alert API and the dashboard time-series view.
CREATE INDEX IF NOT EXISTS idx_scores_model_time
    ON scores (model_name, window_end DESC);

-- Fast per-entity history lookups for the entity detail API.
CREATE INDEX IF NOT EXISTS idx_scores_entity_time
    ON scores (entity_id, window_end DESC);
