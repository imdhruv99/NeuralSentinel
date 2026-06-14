"""
The canonical Kafka message for events.raw / events.scored.

This module is the single source of truth for the on-the-wire shape. Producers
build envelopes; the consumer and scorer will import this exact model so the
contract is defined in one place and validated by pydantic on both ends.
"""


from datetime import datetime, timezone

from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class Dataset(str, Enum):
    """
    Which dataset produced the event.
    Drives model selection downstream.
    """
    NAB = "NAB"
    SMD = "SMD"
    SYNTHETIC = "SYNTHETIC"  # reserved: future proofing for synthetic data generation


class StreamType(str, Enum):
    """
    Tells the consumer how to window the metrics payload.
    """
    UNIVARIATE = "UNIVARIATE"  # NAB: single metric stream, a single `value` per timestamp
    MULTIVARIATE = "MULTIVARIATE"  # SMD: multiple metrics


def _utc_now_iso() -> str:
    """
    Return the current UTC time in ISO 8601 format with a 'Z' suffix.
    """
    # Wall-clock instant the producer published. Used to measure ingestion lag
    # (ingest_ts - timestamp). Timezone-aware so the 'Z'/offset is never ambiguous.
    return datetime.now(timezone.utc).isoformat()


class EventEnvelope(BaseModel):
    """
    The Kafka value for all events. Wraps the original dataset fields with extra
    metadata that the producer adds for all events, like a common schema. This
    metadata is used by the consumer to route events to the right model and to
    manage the LSTM-AE sequence buffer.
    """

    # Unique per event. Generated here so callers can't forget it; consumers use
    # it for idempotent dedup.
    event_id: str = Field(
        default_factory=lambda: str(uuid4()), alias="eventId")

    # Identifies the stream, format "<DATASET>/<stream_name>". This is the Kafka
    # partition key; all events for one stream land on one partition, which is
    # what guarantees per-stream ordering for the LSTM-AE sequence buffer.
    entity_id: str

    dataset: Dataset
    stream_type: StreamType

    # Original measurement time from the dataset, kept as a string. NAB carries
    # real timestamps; SMD has none, so its producer synthesizes them. Storing a
    # string (not datetime) means I don't force one canonical format here.
    timestamp: str

    # Set automatically at construction time.
    ingest_ts: str = Field(default_factory=_utc_now_iso)

    # Ground-truth label from the dataset. None when no label exists (e.g. the
    # SMD train split, or any production stream).
    is_anomaly: bool | None = None

    # Zero-based row index within the stream. Lets the LSTM-AE detect gaps in its
    # sequence buffer (a missing index = a dropped event).
    sequence_idx: int = 0

    # The actual reading(s). NAB -> {"value": 9.6}; SMD -> 38 feature_* floats.
    metrics: dict[str, float]

    def to_json_bytes(self) -> bytes:
        """Serialize to UTF-8 JSON bytes for the Kafka value.

        model_dump_json() emits enum *values* ("NAB", not "Dataset.NAB") because
        Dataset/StreamType subclass `str`, so the wire format stays clean JSON.
        """
        return self.model_dump_json().encode("utf-8")
