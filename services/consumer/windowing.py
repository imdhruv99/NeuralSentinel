"""
Event-time sliding-window feature extraction.

One WindowManager fans events out into per-entity rolling buffers and emits a
FeatureRow each time event-time crosses a slide boundary. Everything here is
pure (no Kafka, no DB) so it can be unit-tested in isolation and reasoned about
without a running stack.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, pstdev

from services.common.contracts import EventEnvelope


def _parse_event_time(ts: str) -> float:
    """
    Event Envelope timestamps are ISO 8601 strings.
    Parse them to Unix timestamps for easier math.

    NAB carries naive local-looking stamps; SMD's producer synthesizes ISO stamps with an explicit +00:00.
    fromisoformat handles both. A naive value is assumed UTC so boundary math is never ambiguous across the two datasets.
    """
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


@dataclass
class FeatureRow:
    """
    One emitted window.
    Maps 1:1 to a row in the `features` table and to the Redis latest-state blob.
    Frozen because once emitted, the consumer treats it as immutable.
    """

    entity_id: str
    dataset: str
    stream_type: str
    window_start: datetime
    window_end: datetime
    event_count: int
    # metric name -> {"mean": x, "stddev": y}
    features: dict[str, dict[str, float]]
    label: bool | None


@dataclass(frozen=True)
class _Sample:
    ts: float  # epoch seconds for easier math
    metrics: dict[str, float]
    is_anomaly: bool | None


def _slope(times: list[float], values: list[float]) -> float:
    """
    Per-second rate of change across the window (last - first)/seconds/.

    Captures trends, which a point-in-time mean misses. Gaurd the degenerate
    single-point / zero-span case so we necer divide by zero.
    """
    span = times[-1] - times[0]
    if span <= 0:
        return 0.0
    return (values[-1] - values[0]) / span


def _compute_features(samples: list[_Sample]) -> dict[str, dict[str, float]]:
    """
    mean/std/min/max/rate per metric key.

    Keys are read from the first sample: A stream's schema is stable
    (NAB= one `value`; SMD = feature_0...feature_37)
    So this avoids unioning keys every call.
    """
    times = [s.ts for s in samples]
    out: dict[str, dict[str, float]] = {}
    for key in samples[0].metrics:
        values = [s.metrics[key] for s in samples]
        out[key] = {
            "mean": mean(values),
            "std": pstdev(values) if len(values) > 1 else 0.0,
            "min": min(values),
            "max": max(values),
            "rate": _slope(times, values),
        }
    return out


def _window_label(samples: list[_Sample]) -> bool | None:
    """
    Ground-truth for the window. True if any event is anomalous; False If all
    are explicitly labeled normal; None if the stream is unlabeled (SMD Train)
    """
    labels = [s.is_anomaly for s in samples]
    if any(v is True for v in labels):
        return True
    if any(v is False for v in labels):
        return False
    return None


class _EntityWindow:
    """
    Rolling buffer + boundary tracker for a single entity_id.
    """

    def __init__(self, dataset: str, stream_type: str,
                 window_size_s: float, slide_s: float) -> None:
        self._dataset = dataset
        self._stream_type = stream_type
        self._window_size_s = window_size_s
        self._slide_s = slide_s
        self._buffer: deque[_Sample] = deque()
        self._next_boundary: float | None = None  # next window_end to emit

    def _emit(self, entity_id: str, window_end: float) -> FeatureRow | None:
        """

        """
        window_start = window_end - self._window_size_s
        members = [s for s in self._buffer if window_start <= s.ts < window_end]
        if not members:
            # a gap larger than the window leaves nothing to summarize, so skip emitting an empty window.
            return None
        return FeatureRow(
            entity_id=entity_id,
            dataset=self._dataset,
            stream_type=self._stream_type,
            window_start=datetime.fromtimestamp(window_start, tz=timezone.utc),
            window_end=datetime.fromtimestamp(window_end, tz=timezone.utc),
            event_count=len(members),
            features=_compute_features(members),
            label=_window_label(members),
        )

    def _evict(self) -> None:
        """
        Drop events no future window can need.
        The next window to emit starts at next_boundary - window_size_s;
        anything older is dead weight.
        """
        if self._next_boundary is None:
            return
        oldest_needed = self._next_boundary - self._window_size_s
        while self._buffer and self._buffer[0].ts < oldest_needed:
            self._buffer.popleft()

    def add(self, entity_id: str, sample: _Sample) -> list[FeatureRow]:
        """
        Add a new sample to the buffer.
        If it crosses the slide boundary, emit a new FeatureRow and advance the window.
        """

        self._buffer.append(sample)

        # First event seeds the boundary grid: the first boundary strictly after
        # this event. Epoch-aligned, so it's independent of where the stream started.
        # Reprocessing yields identical window_end values.
        if self._next_boundary is None:
            self._next_boundary = (
                (sample.ts // self._slide_s) * self._slide_s + self._slide_s
            )

        emmited: list[FeatureRow] = []
        # Watermark has reached/passed pending boundaries:
        # Every event earlier than the boundary has now arrived, so finalize.
        while sample.ts >= self._next_boundary:
            row = self._emit(entity_id, self._next_boundary)
            if row is not None:
                emmited.append(row)
            self._next_boundary += self._slide_s

        self._evict()
        return emmited

    def flush(self, entity_id: str) -> FeatureRow | None:
        """
        Emit the trailing partial window at end-of-stream.

        Without this, the last window_size_s of every stream would never finalize.
        No later event crosses its boundary.
        """
        if self._next_boundary is None:
            return None
        row = self._emit(entity_id, self._next_boundary)
        return [row] if row is not None else []


class WindowManager:
    """
    Manages the rolling windows for all entity_ids.

    Routes envelopes to per-entity windows. This is the only class main.py touches
    add() per message, flush() at shutdown. Everything else is internal.
    """

    def __init__(self, window_size_s: float, slide_s: float) -> None:
        if slide_s <= 0 or window_size_s <= 0:
            raise ValueError("window_size_s and slide_s must be positive")
        if slide_s > window_size_s:
            # slide must be <= size; otherwise events between windows are dropped.
            raise ValueError(
                "slide_s must be less than or equal to window_size_s")
        self._window_size_s = window_size_s
        self._slide_s = slide_s
        self._entities: dict[str, _EntityWindow] = {}

    def add(self, envelope: EventEnvelope) -> list[FeatureRow]:
        """
        Add a new event. If it crosses the slide boundary, emit new FeatureRow(s).

        Returns a list of 0 or more emitted rows. Usually 0 or 1, but if the producer
        sends a batch of events with timestamps that cross multiple boundaries,
        it could be more.
        """

        win = self._entities.get(envelope.entity_id)
        if win is None:
            win = _EntityWindow(
                dataset=envelope.dataset.value,
                stream_type=envelope.stream_type.value,
                window_size_s=self._window_size_s,
                slide_s=self._slide_s,
            )
            self._entities[envelope.entity_id] = win
        sample = _Sample(
            ts=_parse_event_time(envelope.timestamp),
            metrics=envelope.metrics,
            is_anomaly=envelope.is_anomaly,
        )
        return win.add(envelope.entity_id, sample)

    def flush(self) -> list[FeatureRow]:
        """
        Flush all windows at end-of-stream. Emit any trailing partial windows.
        """
        rows: list[FeatureRow] = []
        for entity_id, win in self._entities.items():
            rows.extend(win.flush(entity_id))
        return rows
