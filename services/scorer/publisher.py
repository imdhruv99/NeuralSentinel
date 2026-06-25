"""
Kafka publisher for the scoring pipeline.

Owns the confluent-kafka Producer used to emit scored events and alerts.
Publishing is best-effort: a delivery failure is logged but does not fail the
scoring loop — Postgres (the scores table) is the durable system of record;
Kafka is the low-latency notification layer.

The producer is configured for idempotent delivery (enable.idempotence=true)
to minimise duplicates on the broker side, though at-least-once semantics still
apply.  Consumers of events.scored and alerts should treat message dedup as
their own responsibility.
"""

import logging

from confluent_kafka import KafkaError, Message, Producer

from services.common.contracts import AlertEnvelope, ScoredEnvelope
from services.scorer.config import ScorerConfig

logger = logging.getLogger(__name__)


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        key = msg.key().decode("utf-8", "replace") if msg.key() else "<none>"
        logger.error(
            "delivery failed  topic=%s  key=%s  error=%s",
            msg.topic(),
            key,
            err,
        )


class ScoringPublisher:
    """
    Thin wrapper around confluent-kafka Producer for the two scoring topics.

    Connections are lazy: the underlying socket is established on the first
    produce() call.  Call close() on shutdown to flush the delivery queue and
    release the socket.
    """

    def __init__(self, cfg: ScorerConfig) -> None:
        self._cfg = cfg
        self._producer = Producer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap_servers,
                "client.id": "neural-sentinel-scorer",
                "enable.idempotence": True,
                "acks": "all",
                "retries": 3,
                "linger.ms": 10,
                "compression.type": "lz4",
            }
        )

    def publish_scored(self, envelope: ScoredEnvelope) -> None:
        """Emit a scored event to the events.scored topic."""
        self._producer.produce(
            topic=self._cfg.topic_events_scored,
            key=envelope.entity_id.encode(),
            value=envelope.to_json_bytes(),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)  # trigger delivery callbacks without blocking

    def publish_alert(self, envelope: AlertEnvelope) -> None:
        """Emit an alert to the alerts topic."""
        self._producer.produce(
            topic=self._cfg.topic_alerts,
            key=envelope.entity_id.encode(),
            value=envelope.to_json_bytes(),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout_s: float = 10.0) -> None:
        """Block until all enqueued messages are delivered or timeout expires."""
        remaining = self._producer.flush(timeout=timeout_s)
        if remaining > 0:
            logger.warning(
                "%d message(s) not delivered within %.1fs flush timeout",
                remaining,
                timeout_s,
            )

    def close(self) -> None:
        self.flush()
