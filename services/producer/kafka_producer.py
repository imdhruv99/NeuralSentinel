"""
Shared Kafka publishing layer for the dataset producers.

Both nab_producer and smd_producer build EventEnvelopes; this module owns the
single concern of "getting them onto the broker correctly"; the connection,
the performance knobs from ProducerConfig, and (most importantly) keying every
message by entity_id so per-stream ordering is preserved.
"""

import time

from kafka import KafkaProducer

from services.producer.config import ProducerConfig
from services.common.contracts import EventEnvelope


def build_producer(cfg: ProducerConfig) -> KafkaProducer:
    """
    Construct a KafkaProducer from configuration.

    Serialize in the producers (envelope.to_json()), so value_serializer
    is left as identity bytes. The key is the entity_id encoded to UTF-8.
    Kafka hashes the key bytes to choose the partition, so encoding must be stable.
    """

    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers.split(","),
        client_id="neural-sentinel-producer",

        # --- delivery guarantees ---
        # acks="all": wait for all in-sync replicas to ack before considering the
        # send successful. With RF=2 this means both replicas have the record.
        # no silent data loss if a broker dies right after publish.
        acks="all",

        # Bounded retries for transient errors (leader election, brief timeouts).
        retries=3,

        # With idempotence on, retries can't create duplicates or reorder records
        # within a partition. Costs nothing meaningful for our throughput.
        enable_idempotence=True,

        # --- throughput knobs ---
        batch_size=cfg.producer_batch_size,
        linger_ms=cfg.producer_linger_ms,
        compression_type=(
            None if cfg.producer_compression == "none" else cfg.producer_compression
        ),
    )


def publish(producer: KafkaProducer, topic: str, event: EventEnvelope) -> None:
    """
    Publish one envelope, keyed by entity_id to the given topic.

    The key is what enforces the ordering invariant: kafka computes partition = hash(key) % num_partitions,
    so every event for a given stream lands on the same partition and is therefore consumed in order.
    This is hard requirement for the LSTM-AE sequence buffer to work correctly.

    I do not block on the returned future here, that would serialize every send and destroy throughput.
    The producer batches in the background; the caller flushes once at the end of the replay.
    """

    producer.send(
        topic,
        key=event.entity_id.encode("utf-8"),
        value=event.to_json_bytes(),
    )


def flush_and_close(producer: KafkaProducer) -> None:
    """
    Flush buffered records and close the producer cleanly.

    flush() blocks until every buffered record has been acked (or finally failed).
    Always call this before exiting the process, otherwise some records still sitting in the
    in-memory batch are lost when the process ends.
    """

    producer.flush()
    producer.close()
