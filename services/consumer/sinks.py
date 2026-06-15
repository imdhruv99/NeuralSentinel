"""
Durable + cache sinks for feature rows.

Postgres is the system of record (offline feature store, idempotent upsert).
Redis is a best-effort online cache holding the latest window per entity for.
The consumer persists a batch here and only commits Kafka offsets after
write_batch() returns -> persist-before-commit gives effectively-once semantics
on top of at-least-once delivery.
"""

import json
import logging

import redis
import psycopg
from psycopg.types.json import Json

from services.consumer.config import ConsumerConfig
from services.consumer.windowing import FeatureRow

logger = logging.getLogger(__name__)


_UPSERT_SQL = """
INSERT INTO features (
    entity_id,
    dataset,
    stream_type,
    window_start,
    window_end,
    event_count,
    features,
    label
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (entity_id, window_end) DO NOTHING
"""


class FeatureSink:
    """
    Owns the Postgres connection and Redis client for the consumer's lifetime.

    Connections are opended once in connect() and reused for every batch;
    opening per batch would dominate latency.
    Call close() on shutdown.
    """

    def __init__(self, cfg: ConsumerConfig):
        self._cfg = cfg
        self._pg: psycopg.Connection | None = None
        self._redis: redis.Redis | None = None

    def connect(self) -> None:
        # autocommit=False: each write_batch() is one explicit transaction we
        # commit only after every row is staged -> a mid-batch crash rolls the
        # whole batch back, and the uncommitted Kafka offset replays it.
        # The batch transaction is the unit of durability. The loop's contract
        # "batch is durable when write_batch() returns" relies on this.
        # An explicit commit at the end of the batch delivers exactly that,
        # and any failure before it rolls back cleanly so the un-committed
        # Kafka offsets replay the same rows.
        self._pg = psycopg.connect(self._cfg.pg_dsn, autocommit=False)
        self._redis = redis.Redis(
            host=self._cfg.redis_host,
            port=self._cfg.redis_port,
            db=self._cfg.redis_db,
            password=self._cfg.redis_password,
            socket_timeout=2,  # cache must never hang the hot path
            decode_responses=False,
        )

    def close(self) -> None:
        if self._pg is not None:
            self._pg.close()
        if self._redis is not None:
            self._redis.close()

    def write_batch(self, rows: list[FeatureRow]) -> int:
        """
        Persist a batch durably. Returns rows actually inserted (conflicts excluded).
        Raises on DB error so the caller skips the offset commit and the batch replays.
        """

        if not rows:
            return 0
        assert self._pg is not None, "connect() must be called before write_batch()"

        params = [
            (
                r.entity_id,
                r.dataset,
                r.stream_type,
                r.window_start,
                r.window_end,
                r.event_count,
                Json(r.features),  # dict -> JSONB
                r.label,
            )
            for r in rows
        ]

        with self._pg.cursor() as cur:
            cur.executemany(_UPSERT_SQL, params)
            # -1 if the driver can't report; treat as unknown but successful insert
            inserted = cur.rowcount
        self._pg.commit()

        # Cache update is deliberately AFTER the durable commit: the record of
        # truth wins, the cache merely reflects it.
        # Why? If the Redis is source of truth, eventually it will serve stale or phantom data.
        # Postgres commits first; the cache is projection of what's already durable.
        self._update_cache(rows)
        return inserted

    def _update_cache(self, rows: list[FeatureRow]) -> None:
        """
        Write the latest window per entity to Redis. Best-effort: A cache failure is logged,
        never raised - It must not undo a durable write or block the offset commit.
        """

        if self._redis is None:
            return

        # A batch can hold several windows for one entity; only the last
        # (highest window_end) is "latest", so collapse first to one SET each.
        latest: dict[str, FeatureRow] = {}
        for r in rows:
            cur = latest.get(r.entity_id)
            if cur is None or r.window_end > cur.window_end:
                latest[r.entity_id] = r

        try:
            pipe = self._redis.pipeline(transaction=False)
            for entity_id, r in latest.items():
                pipe.set(f"features:latest:{entity_id}", self._to_blob(r))
            pipe.execute()
        except redis.RedisError as e:
            logger.warning(
                "Redis cache update failed for %d entities: %s", len(latest), e, exc_info=True)

    @staticmethod
    def _to_blob(r: FeatureRow) -> bytes:
        """
        Serialize a FeatureRow to a Redis blob. I use JSON for readability and debugging,
        but could switch to MessagePack or similar if size or speed becomes an issue.
        """

        return json.dumps({
            "entity_id": r.entity_id,
            "dataset": r.dataset,
            "stream_type": r.stream_type,
            "window_start": r.window_start.isoformat(),
            "window_end": r.window_end.isoformat(),
            "event_count": r.event_count,
            "features": r.features,
            "label": r.label,
        }).encode("utf-8")
