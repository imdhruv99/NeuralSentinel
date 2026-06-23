"""
Feature-windowing consumer entrypoint (RTAD-004).

Glues the three pure/IO pieces together:
  events.raw  --poll-->  WindowManager  --FeatureRow-->  FeatureSink

The loop is deliberately persist-before-commit: a batch is written durably to
Postgres *before* its Kafka offsets are committed, so a crash replays the batch
and ON CONFLICT DO NOTHING dedups it. At-least-once delivery + idempotent writes
= effectively-once features.
"""

import sys
import logging
import signal

from confluent_kafka import Consumer

from config.logging import setup_logging
from services.common.contracts import EventEnvelope
from services.consumer.config import ConsumerConfig
from services.consumer.windowing import FeatureRow, WindowManager
from services.consumer.sinks import FeatureSink

logger = logging.getLogger(__name__)


class _Shutdown:
    """
    Flips on SIGINT/SIGTERM so the loop drains and commits the current batch
    instead of dying mid-flight and replaying it.
    Cooperative, not forced.
    """

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, _frame) -> None:
        logger.info("Shutdown signal %s received; draining...", signum)
        self.requested = True


def build_consumer(cfg: ConsumerConfig) -> Consumer:
    """
    Manual-commit consumer. auto.offset.reset='earliest' so a fresh group
    replays the whole topic (we want every historical event windowed).

    librdkafka takes a flat config dict; subscribe() (not assign()) lets the
    group coordinator hand out partitions and rebalance across consumers.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": cfg.consumer_group,
            "enable.auto.commit": False,   # we own the commit, after persistence
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([cfg.topic_events_raw])
    return consumer


def run(cfg: ConsumerConfig) -> None:
    shutdown = _Shutdown()
    sink = FeatureSink(cfg)
    sink.connect()
    manager = WindowManager(cfg.window_size_s, cfg.slide_s)
    consumer = build_consumer(cfg)

    pending: list[FeatureRow] = []
    total_in = total_out = 0
    # confluent's commit() raises if no offsets have been stored yet, so only
    # commit once we've actually consumed something since the last commit.
    consumed_since_commit = 0

    def flush_pending() -> None:
        """Persist accumulated rows, then commit offsets. Order is the whole
        point: durable first, commit second."""
        nonlocal pending, consumed_since_commit
        if pending:
            sink.write_batch(pending)
            pending = []
        if consumed_since_commit:
            consumer.commit(asynchronous=False)
            consumed_since_commit = 0

    try:
        while not shutdown.requested:
            # confluent poll() returns ONE message or None; the timeout (seconds)
            # bounds the wait so we periodically re-check the shutdown flag.
            msg = consumer.poll(cfg.poll_timeout_ms / 1000.0)
            if msg is None:
                continue
            if msg.error() is not None:
                # Transient/EOF conditions surface here, not as exceptions.
                logger.warning("consumer error: %s", msg.error())
                continue

            total_in += 1
            consumed_since_commit += 1
            try:
                envelope = EventEnvelope.model_validate_json(msg.value())
            except Exception:
                # One poison message must not stop the stream.
                logger.error("skipping unparseable message at offset %s",
                             msg.offset(), exc_info=True)
                continue
            rows = manager.add(envelope)
            pending.extend(rows)
            total_out += len(rows)

            # Batch boundary: persist + commit once the buffer is large enough.
            # This is the backpressure lever — bigger batch = fewer commits,
            # more memory held before durability.
            if len(pending) >= cfg.batch_max_rows:
                flush_pending()

        # Graceful shutdown: emit trailing partial windows, then final flush.
        logger.info("draining trailing windows...")
        before = len(pending)
        pending.extend(manager.flush())
        total_out += len(pending) - before
        flush_pending()

    finally:
        logger.info("consumed=%s emitted=%s; closing", total_in, total_out)
        consumer.close()
        sink.close()


def main() -> int:
    setup_logging()
    run(ConsumerConfig())
    return 0


if __name__ == "__main__":
    sys.exit(main())
