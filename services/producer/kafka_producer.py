"""
Shared Kafka publishing layer for the dataset producers.

Both nab_producer and smd_producer build EventEnvelopes; this module owns the
single concern of "getting them onto the broker correctly"; the connection,
the performance knobs from ProducerConfig, and (most importantly) keying every
message by entity_id so per-stream ordering is preserved.

Backed by confluent-kafka (librdkafka). Unlike kafka-python's send(), produce()
enqueues into a bounded local buffer and is fully asynchronous; delivery results
arrive on a background thread and are surfaced through _on_delivery.
"""

import logging

from confluent_kafka import Producer

from services.producer.config import ProducerConfig
from services.common.contracts import EventEnvelope

logger = logging.getLogger(__name__)


def _on_delivery(err, msg) -> None:
    """Per-message delivery report (called from poll()/flush()).

    Success is silent; a failure here means the record never reached the broker
    after all retries, so it's logged loudly. The key is included because that's
    the entity_id, which tells us which stream lost data."""
    if err is not None:
        key = msg.key().decode("utf-8", "replace") if msg.key() else "<none>"
        logger.error("delivery failed for key=%s: %s", key, err)


def build_producer(cfg: ProducerConfig) -> Producer:
    """
    Construct a confluent-kafka Producer from configuration.

    librdkafka takes a flat config dict with dotted keys. Values are serialized
    by the caller (envelope.to_json_bytes()), so there's no value serializer
    here. bootstrap.servers accepts the raw comma-separated string directly.
    """

    conf = {
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "client.id": "neural-sentinel-producer",

        # --- delivery guarantees ---
        # enable.idempotence implies acks=all and bounds in-flight requests so
        # retries can't duplicate or reorder within a partition. librdkafka sets
        # acks=all itself, but we state it for documentation.
        "enable.idempotence": True,
        "acks": "all",
        "retries": 3,

        # --- throughput knobs ---
        # linger.ms lets librdkafka accumulate a batch before sending; compression
        # is applied per batch. Our JSON compresses ~4-6x.
        "linger.ms": cfg.producer_linger_ms,
        "batch.size": cfg.producer_batch_size,
        "compression.type": cfg.producer_compression,  # "none" is valid here
    }
    return Producer(conf)


def publish(producer: Producer, topic: str, event: EventEnvelope) -> None:
    """
    Publish one envelope, keyed by entity_id to the given topic.

    The key enforces the ordering invariant: Kafka computes partition =
    hash(key) % num_partitions, so every event for a stream lands on the same
    partition and is consumed in order. Hard requirement for the LSTM-AE buffer.

    produce() is asynchronous: it enqueues into librdkafka's local buffer and
    returns immediately. That buffer is bounded (queue.buffering.max.messages),
    so a fast replay can fill it -> produce() raises BufferError. The fix is to
    poll(), which serves delivery callbacks and drains the queue, then retry.
    The trailing poll(0) is non-blocking and just keeps callbacks flowing.
    """

    while True:
        try:
            producer.produce(
                topic,
                key=event.entity_id.encode("utf-8"),
                value=event.to_json_bytes(),
                on_delivery=_on_delivery,
            )
            break
        except BufferError:
            # Queue full: block briefly to let in-flight sends complete, freeing
            # space, then retry the same record.
            producer.poll(0.5)
    producer.poll(0)


def flush_and_close(producer: Producer) -> None:
    """
    Flush buffered records before exiting.

    flush() blocks until every queued record has a delivery result (acked or
    finally failed) and returns the count still in queue. confluent-kafka has no
    explicit close() — flushing is what guarantees nothing buffered is lost when
    the process ends.
    """

    remaining = producer.flush(30)
    if remaining > 0:
        logger.warning(
            "%d message(s) still undelivered after flush timeout", remaining)
