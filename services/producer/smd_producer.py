import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.producer.config import ProducerConfig
from services.common.contracts import EventEnvelope, Dataset, StreamType
from services.producer.kafka_producer import build_producer, publish, flush_and_close

# SMD rows are 38-dimensional vectors of sensor readings.
# Name the metrics positionally because the dataset ships without headers.
SMD_NUM_FEATURES = 38

# SMD has no timestamps, only relative row numbers.
# I assign timestamps starting at 01-01-2020 00:00:00 UTC and
# incrementing by 1 minute per row.
# This is an arbitrary choice, but it gives us a real datetime to work with and
# is easy to verify in the replay that timestamps are correct and in order.
SMD_EPOCH = datetime(2020, 1, 1, tzinfo=timezone.utc)
SMD_INTERVAL = timedelta(minutes=1)


def split_dirs(cfg: ProducerConfig) -> tuple[Path, Path | None]:
    """
    Return (data_subdir, labels_subdir) based on the configured split.

    Test split -> use test/ + test_labels/
    Train split -> use train/ only
    """
    root = cfg.smd_data_dir
    if cfg.smd_use_test_split:
        return root / "test", root / "test_labels"
    return root / "train", None


def discover_machines(data_subdir: Path, machine_filter: str | None) -> list[Path]:
    """
    Find every machine directory under the data subdir, optionally filtered by substring.

    The filter lets us replay a single machine for fast local testing instead of all 28.
    """

    machines = sorted(data_subdir.glob("*.txt"))
    if machine_filter:
        machines = [m for m in machines if machine_filter in m.stem]
    return machines


def load_labels(label_file: Path | None) -> list[int] | None:
    """
    Load a test_label file into a row-aligned list of 0/1 labels,
    or return None if no label file is provided.

    Each line is a single integer. The list index == the row index in the matching
    test/ file, so labels[i] is the ground truth for row i.
    """
    if label_file is None or not label_file.exists():
        return None

    return [int(line.strip()) for line in label_file.read_text().splitlines() if line.strip()]


def row_to_metrics(row: list[str]) -> dict[str, float]:
    """
    Turn a 38-dimensional SMD row into dict of metric name to value

    Validate the width: A short/long row means a parsing or data problem want to fail
    fast and loud instead of silently producing bad data.
    """
    if len(row) != SMD_NUM_FEATURES:
        raise ValueError(
            f"Expected {SMD_NUM_FEATURES} features, got {len(row)}: {row[:3]}...")

    return {f"feature_{i}": float(value) for i, value in enumerate(row)}


def replay_machine(
    producer,
    cfg: ProducerConfig,
    txt_path: Path,
    labels: list[int] | None,
) -> int:
    """
    Replay one machine file to events.raw. Returns the number of events published.

    entity_id is "SMD/<machine_name>" so it reads cleanly and stays stable as the partition key.
    timestamp is synthesized as SMD_EPOCH + row_index * SMD_INTERVAL,
    which gives us a real datetime to work with.
    is_anomaly is read from the labels file if provided, otherwise set to None for all rows.
    """

    machine = txt_path.stem
    entity_id = f"SMD/{machine}"

    count = 0
    with txt_path.open(newline="") as f:
        reader = csv.reader(f)  # plain reader: no headers in SMD files
        for idx, row in enumerate(reader):
            if not row:
                continue  # skip empty lines if any

            is_anomaly = None
            if labels is not None and idx < len(labels):
                is_anomaly = bool(labels[idx])

            event = EventEnvelope(
                entity_id=entity_id,
                dataset=Dataset.SMD,
                stream_type=StreamType.MULTIVARIATE,
                timestamp=(SMD_EPOCH + idx * SMD_INTERVAL).isoformat(),
                metrics=row_to_metrics(row),
                is_anomaly=is_anomaly,
                sequence_idx=idx,
            )

            publish(producer, cfg.topic_events_raw, event)
            count += 1

            if cfg.smd_replay_speed > 0:
                time.sleep(cfg.smd_replay_speed)

    return count


def main() -> None:
    cfg = ProducerConfig()
    data_subdir, labels_subdir = split_dirs(cfg)
    machines = discover_machines(data_subdir, cfg.smd_machine_filter)

    if not machines:
        raise SystemExit(
            f"No machine files found in {data_subdir}"
            f"(filter={cfg.smd_machine_filter!r}). Did you run `make fetch-data`?"
            f"Please run `make fetch-data` to download the SMD dataset before replaying."
        )

    split = "test" if cfg.smd_use_test_split else "train"
    print(f"Replaying {len(machines)} machine(s) from SMD {split} split to {cfg.topic_events_raw} at {cfg.smd_replay_speed}x"
          f" speed")

    producer = build_producer(cfg)
    total = 0
    try:
        for txt_path in machines:
            labels = None
            if labels_subdir is not None:
                labels = load_labels(labels_subdir / txt_path.name)

            n = replay_machine(producer, cfg, txt_path, labels)
            print(f"Replayed {n} events from {txt_path.name}")
            total += n
    finally:
        flush_and_close(producer)

    print(f"Done. Published {total} SMD events to {cfg.topic_events_raw}.")


if __name__ == "__main__":
    main()
