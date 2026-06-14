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
