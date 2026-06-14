"""
Replay the Numenta Anomaly Benchmark (NAB) into events.raw.

NAB is univariate: each stream is a CSV of (timestamp, value). Ground-truth
anomalies are point labels; a JSON file lists the exact timestamps that are
anomalous per stream. Let us walk every CSV row-by-row, wrap it in an EventEnvelope
keyed by entity_id, and publish at a configurable replay speed.
"""

import csv
import json
import time
from pathlib import Path

from services.producer.config import ProducerConfig
from services.common.contracts import EventEnvelope, Dataset, StreamType
from services.producer.kafka_producer import build_producer, publish, flush_and_close


def load_labels(labels_file: Path) -> dict[str, set[str]]:
    """
    Load combined_labels.json into {relative_csv_path: {anomaly_timestamps}}.

    The file stores anomaly timestamps as a JSON list per stream.
    I convert each list to a set of the per-row membership check below is O(1) instead of O(n).
    Streams can have thousands of rows and I test every one, so this is a meaningful optimization.
    """
    raw = json.loads(labels_file.read_text())
    return {
        stream: set(timestamps)
        for stream, timestamps in raw.items()
    }


def discover_streams(data_dir: Path, stream_filter: str | None) -> list[Path]:
    """
    Find every NAB CSV under data_dir, optionally filtered by substring.

    "rglob" is recursive globbing, it walks the category subdirectories.
    The filter lets you replay a single stream for local testing instead of all 58.
    """
    streams = sorted(data_dir.rglob("*.csv"))
    if stream_filter:
        streams = [s for s in streams if stream_filter in str(s)]
    return streams


def relative_stream_key(data_dir: Path, csv_path: Path) -> str:
    """
    Build the label-file key for CSV: `<category>/<file>.csv`

    combined_labels.json keys are relative to the NAB data dir.
    So make the path relative to the data_dir to look labels up correctly.
    The relative path is also a stable entity_id for the stream.
    """
    return str(csv_path.relative_to(data_dir))


def replay_stream(
    producer,
    cfg: ProducerConfig,
    csv_path: Path,
    anomaly_timestamps: set[str],
) -> int:
    """
    Replay one CSV to events.raw. Returns the number of events published.

    entity_id is "NAB/<category>/<stream>" (no .csv) so it reads cleanly and stays stable
    as the partition key.
    sequence_idx is the row's position in the stream, which the LSTM-AE side can use to
    detect gaps in the sequence.
    """

    stream_key = relative_stream_key(cfg.nab_data_dir, csv_path)
    entity_id = f"NAB/{stream_key.removesuffix('.csv')}"

    count = 0
    with csv_path.open(newline="") as f:
        # uses the 'timestamp' and 'value' as headers
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            ts = row["timestamp"]
            event = EventEnvelope(
                entity_id=entity_id,
                dataset=Dataset.NAB,
                stream_type=StreamType.UNIVARIATE,
                timestamp=ts,
                metrics={"value": float(row["value"])},
                is_anomaly=ts in anomaly_timestamps,
                sequence_idx=idx,
            )

            publish(producer, cfg.topic_events_raw, event)
            count += 1

            # Sleep to control replay speed
            if cfg.nab_replay_speed > 0:
                time.sleep(cfg.nab_replay_speed)

    return count


def main() -> None:

    cfg = ProducerConfig()
    labels = load_labels(cfg.nab_labels_file)
    streams = discover_streams(cfg.nab_data_dir, cfg.nab_stream_filter)

    if not streams:
        raise SystemExit(
            f"No NAB CSVs found under {cfg.nab_data_dir}"
            f"(filter={cfg.nab_stream_filter!r}). Did you run `make fetch-data`?"
            "Please run `make fetch-data` to download the NAB dataset before replaying."
        )

    print(
        f"Replaying {len(streams)} NAB streams to {cfg.topic_events_raw} at {cfg.nab_replay_speed}x speed")

    producer = build_producer(cfg)
    total = 0

    try:
        for csv_path in streams:
            stream_key = relative_stream_key(cfg.nab_data_dir, csv_path)
            n = replay_stream(producer, cfg, csv_path,
                              labels.get(stream_key, set()))
            print(f"{stream_key}: published {n} events")
            total += n
    finally:
        flush_and_close(producer)

    print(f"Done. Published {total} NAB events to {cfg.topic_events_raw}.")


if __name__ == "__main__":
    main()
