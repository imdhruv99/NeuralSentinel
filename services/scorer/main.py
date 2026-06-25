"""
Real-time anomaly scoring service.

Polls Postgres for feature windows that have not yet been scored, runs
each window through the appropriate Production model, writes the results to
the scores table, and publishes ScoredEnvelopes to events.scored.  Any window
whose score crosses the anomaly threshold is additionally deduplicated via
Redis and published to the alerts topic.

Dataset <-> model routing
-----------------------
  NAB / UNIVARIATE  ->  neural-sentinel-isolation-forest-model
  SMD / MULTIVARIATE ->  neural-sentinel-lstm-autoencoder-model

Delivery semantics
------------------
The scores table is the durable record.  DB writes happen inside an explicit
transaction; Kafka publishes are best-effort and happen after the commit.  A
crash between the commit and the Kafka produce will replay scoring on restart
(the window will appear in the unscored query again), but because the score row
already exists the DB write will be a no-op (PK conflict) and only the Kafka
side is re-sent - an acceptable at-least-once outcome for the notification path.
"""

import json
import logging
import signal
import time
from dataclasses import dataclass

import psycopg
import redis

from config.logging import setup_logging
from services.common.contracts import AlertEnvelope, Dataset, ScoredEnvelope
from services.scorer.alert_dedup import AlertDedup
from services.scorer.config import ScorerConfig
from services.scorer.model_registry import ModelRegistry
from services.scorer.publisher import ScoringPublisher
from services.scorer.scorer import ScoredRow, score_iforest, score_lstm

logger = logging.getLogger(__name__)


# Watermark helpers — persisted in Redis so restarts resume from where they
# left off rather than re-scoring the entire feature store.
_WATERMARK_KEY_PREFIX = "scorer:watermark"


def _load_watermark(r: redis.Redis, model_name: str) -> str:
    """
    Return the last successfully scored window_end for a given model as an
    ISO 8601 string, or a sentinel that causes all rows to be fetched on the
    first run.
    """
    raw = r.get(f"{_WATERMARK_KEY_PREFIX}:{model_name}")
    return raw.decode() if raw else "1970-01-01T00:00:00+00:00"


def _save_watermark(r: redis.Redis, model_name: str, watermark: str) -> None:
    r.set(f"{_WATERMARK_KEY_PREFIX}:{model_name}", watermark)


# Postgres queries
_FETCH_UNSCORED_SQL = """
SELECT
    f.entity_id,
    f.dataset,
    f.stream_type,
    f.window_end,
    f.features
FROM features f
WHERE f.dataset = %(dataset)s
  AND f.window_end > %(watermark)s
  AND NOT EXISTS (
      SELECT 1
      FROM   scores s
      WHERE  s.entity_id  = f.entity_id
        AND  s.window_end = f.window_end
        AND  s.model_name = %(model_name)s
  )
ORDER BY f.window_end ASC
LIMIT %(batch_size)s
"""

# Fetch the last seq_len windows per entity for LSTM sequence construction.
# The LATERAL join keeps this a single round-trip regardless of entity count.
_FETCH_SEQUENCES_SQL = """
SELECT
    e.entity_id,
    seq.window_end,
    seq.features
FROM (SELECT DISTINCT entity_id FROM features WHERE entity_id = ANY(%(entity_ids)s)) e
CROSS JOIN LATERAL (
    SELECT window_end, features
    FROM   features
    WHERE  entity_id = e.entity_id
      AND  dataset   = 'SMD'
      AND  window_end <= %(max_window_end)s
    ORDER  BY window_end DESC
    LIMIT  %(seq_len)s
) seq
ORDER BY e.entity_id, seq.window_end ASC
"""

_INSERT_SCORE_SQL = """
INSERT INTO scores (
    entity_id, window_end, model_name, model_version,
    anomaly_score, is_anomaly
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (entity_id, window_end, model_name) DO NOTHING
"""


# Shutdown sentinel
class _Shutdown:
    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, _frame) -> None:
        logger.info("shutdown signal %s received; draining...", signum)
        self.requested = True


# Per-cycle helpers
def _fetch_unscored(
    conn: psycopg.Connection,
    dataset: str,
    model_name: str,
    watermark: str,
    batch_size: int,
) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            _FETCH_UNSCORED_SQL,
            {
                "dataset": dataset,
                "watermark": watermark,
                "model_name": model_name,
                "batch_size": batch_size,
            },
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_sequences(
    conn: psycopg.Connection,
    entity_ids: list[str],
    max_window_end: object,
    seq_len: int,
) -> dict[str, list[dict]]:
    """
    Return {entity_id: [feature_map, ...]} ordered oldest→newest per entity.
    Each list has at most seq_len entries.
    """
    with conn.cursor() as cur:
        cur.execute(
            _FETCH_SEQUENCES_SQL,
            {
                "entity_ids": entity_ids,
                "max_window_end": max_window_end,
                "seq_len": seq_len,
            },
        )
        sequences: dict[str, list[dict]] = {}
        for entity_id, _window_end, features in cur.fetchall():
            fmap = json.loads(features) if isinstance(
                features, str) else features
            sequences.setdefault(entity_id, []).append(fmap)
    return sequences


def _write_scores(conn: psycopg.Connection, rows: list[ScoredRow]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            _INSERT_SCORE_SQL,
            [
                (
                    r.entity_id,
                    r.window_end,
                    r.model_name,
                    r.model_version,
                    r.anomaly_score,
                    r.is_anomaly,
                )
                for r in rows
            ],
        )
    conn.commit()


def _emit(
    row: ScoredRow,
    publisher: ScoringPublisher,
    dedup: AlertDedup,
) -> None:
    scored_env = ScoredEnvelope(
        entity_id=row.entity_id,
        dataset=Dataset(row.dataset),
        stream_type=row.stream_type,  # type: ignore[arg-type]
        window_end=row.window_end.isoformat(),
        model_name=row.model_name,
        model_version=row.model_version,
        anomaly_score=row.anomaly_score,
        is_anomaly=row.is_anomaly,
    )
    publisher.publish_scored(scored_env)

    if row.is_anomaly and dedup.should_alert(row.model_name, row.entity_id):
        alert_env = AlertEnvelope(
            entity_id=row.entity_id,
            dataset=Dataset(row.dataset),
            model_name=row.model_name,
            model_version=row.model_version,
            anomaly_score=row.anomaly_score,
            window_end=row.window_end.isoformat(),
        )
        publisher.publish_alert(alert_env)
        logger.info(
            "alert  entity=%s  model=%s  score=%.4f",
            row.entity_id,
            row.model_name,
            row.anomaly_score,
        )


# Core cycle
@dataclass
class _CycleStats:
    nab_scored: int = 0
    smd_scored: int = 0
    alerts_fired: int = 0


def _run_cycle(
    cfg: ScorerConfig,
    registry: ModelRegistry,
    publisher: ScoringPublisher,
    dedup: AlertDedup,
    redis_client: redis.Redis,
) -> int:
    """Execute one poll cycle. Returns total rows scored."""
    iforest = registry.get(cfg.iforest_model_name)
    lstm = registry.get(cfg.lstm_model_name)

    iforest_wm = _load_watermark(redis_client, cfg.iforest_model_name)
    lstm_wm = _load_watermark(redis_client, cfg.lstm_model_name)

    total = 0

    with psycopg.connect(cfg.pg_dsn) as conn:

        # NAB path - Isolation Forest
        if iforest is not None:
            raw_rows = _fetch_unscored(
                conn, "NAB", cfg.iforest_model_name, iforest_wm, cfg.batch_size
            )
            scored_rows: list[ScoredRow] = []
            for r in raw_rows:
                fmap = json.loads(r["features"]) if isinstance(
                    r["features"], str) else r["features"]
                try:
                    scored = score_iforest(
                        iforest,
                        r["entity_id"],
                        r["dataset"],
                        r["stream_type"],
                        r["window_end"],
                        fmap,
                    )
                    scored_rows.append(scored)
                except Exception:
                    logger.exception(
                        "iforest scoring failed for entity=%s", r["entity_id"])

            if scored_rows:
                _write_scores(conn, scored_rows)
                for sr in scored_rows:
                    _emit(sr, publisher, dedup)
                new_wm = max(sr.window_end for sr in scored_rows).isoformat()
                _save_watermark(redis_client, cfg.iforest_model_name, new_wm)
                total += len(scored_rows)
                logger.info(
                    "iforest  scored=%d  anomalies=%d  watermark=%s",
                    len(scored_rows),
                    sum(1 for s in scored_rows if s.is_anomaly),
                    new_wm,
                )

        # SMD path - LSTM Autoencoder
        if lstm is not None:
            seq_len: int = lstm.calibration["seq_len"]
            raw_rows = _fetch_unscored(
                conn, "SMD", cfg.lstm_model_name, lstm_wm, cfg.batch_size
            )
            if raw_rows:
                entity_ids = list({r["entity_id"] for r in raw_rows})
                max_window_end = max(r["window_end"] for r in raw_rows)
                sequences = _fetch_sequences(
                    conn, entity_ids, max_window_end, seq_len)

                scored_rows = []
                for r in raw_rows:
                    fmap = json.loads(r["features"]) if isinstance(
                        r["features"], str) else r["features"]
                    seq = sequences.get(r["entity_id"], [])
                    # Ensure the target window is the tail of the sequence
                    if seq and seq[-1] != fmap:
                        seq = seq + [fmap]
                    seq = seq[-seq_len:]  # keep at most seq_len entries
                    try:
                        scored = score_lstm(
                            lstm,
                            r["entity_id"],
                            r["dataset"],
                            r["stream_type"],
                            r["window_end"],
                            seq,
                        )
                        scored_rows.append(scored)
                    except Exception:
                        logger.exception(
                            "lstm scoring failed for entity=%s", r["entity_id"])

                if scored_rows:
                    _write_scores(conn, scored_rows)
                    for sr in scored_rows:
                        _emit(sr, publisher, dedup)
                    new_wm = max(
                        sr.window_end for sr in scored_rows).isoformat()
                    _save_watermark(redis_client, cfg.lstm_model_name, new_wm)
                    total += len(scored_rows)
                    logger.info(
                        "lstm     scored=%d  anomalies=%d  watermark=%s",
                        len(scored_rows),
                        sum(1 for s in scored_rows if s.is_anomaly),
                        new_wm,
                    )

    return total


# Entry point
def run(cfg: ScorerConfig) -> None:
    shutdown = _Shutdown()

    redis_client = redis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        db=cfg.redis_db,
        password=cfg.redis_password,
        socket_timeout=2,
        decode_responses=False,
    )

    registry = ModelRegistry(cfg)
    logger.info("loading Production models from MLflow...")
    registry.warm_up()

    publisher = ScoringPublisher(cfg)
    dedup = AlertDedup(redis_client, cfg.alert_cooldown_s)

    cycle = 0
    logger.info(
        "scoring loop started  poll_interval=%.1fs  batch_size=%d  alert_cooldown=%ds",
        cfg.poll_interval_s,
        cfg.batch_size,
        cfg.alert_cooldown_s,
    )

    while not shutdown.requested:
        cycle += 1

        if cycle % cfg.model_refresh_cycles == 0:
            registry.refresh()

        try:
            scored = _run_cycle(cfg, registry, publisher, dedup, redis_client)
        except Exception:
            logger.exception("scoring cycle %d failed — will retry", cycle)
            scored = 0

        if scored == 0:
            time.sleep(cfg.poll_interval_s)

    logger.info("shutdown: flushing Kafka producer...")
    publisher.close()
    redis_client.close()
    logger.info("scorer stopped cleanly")


def main() -> None:
    setup_logging()
    cfg = ScorerConfig()
    run(cfg)


if __name__ == "__main__":
    main()
