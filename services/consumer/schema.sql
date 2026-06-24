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
