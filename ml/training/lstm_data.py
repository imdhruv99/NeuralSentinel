
import json

import numpy as np
import pandas as pd
import psycopg

from ml.training.lstm_config import LSTMAEConfig


def _flatten_feature_map(feature_map: dict) -> dict[str, float]:
    """
    Flatten the nested feature map in SMD's .txt files into a single dict of metric name to value.

    Args:
        - feature_map: A dict where keys are metric names and values are dicts of statistic names to values.

    Returns:
        - A flat dict where keys are "<metric_name>_<stat_name>" and values are the corresponding float values.
    """
    row: dict[str, float] = {}
    for metric_name, stats_map in feature_map.items():
        for stat_name, value in stats_map.items():
            row[f"{metric_name}_{stat_name}"] = value
    return row


def load_squence_frame(cfg: LSTMAEConfig) -> pd.DataFrame:
    """
    Load the preprocessed features from Postgres into a flat DataFrame for LSTM-AE training.
    The features are stored in a JSONB column as a nested dict of metric to stats, so we flatten them into
    a single level of metric_stat -> value columns. We also include the entity_id, window_end, and label columns.

    Args:
        - cfg: LSTMAEConfig containing the Postgres connection info and dataset/stream

    Returns:
        - A pandas DataFrame with one row per window, columns for each flattened metric_stat,
        and columns for entity_id, window_end, and label.
    """
    sql = """
        SELECT entity_id, window_end, features, label
        FROM features
        WHERE dataset = %(dataset)s AND stream_type = %(stream_type)s
        ORDER BY entity_id, window_end ASC
    """
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"dataset": cfg.dataset,
                        "stream_type": cfg.stream_type})
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        raise ValueError(
            f"No data found for dataset={cfg.dataset} and stream_type={cfg.stream_type}. "
            f"Check your producer is running and producing to the right topic."
        )

    flat_rows = []
    for _, row in df.iterrows():
        f_map = row["features"]
        if isinstance(f_map, str):
            f_map = json.loads(f_map)
        flat = _flatten_feature_map(f_map)
        flat["entity_id"] = row["entity_id"]
        flat["window_end"] = row["window_end"]
        flat["label"] = row["label"]
        flat_rows.append(flat)

    return pd.DataFrame(flat_rows)


def make_train_val_sequences(
    frame: pd.DataFrame,
    feature_cols: list[str],
    seq_len: int,
    validation_ratio: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:

    train_seq, train_labels = [], []
    val_seq, val_labels = [], []

    for _, group in frame.groupby("entity_id", sort=False):
        x = group[feature_cols].to_numpy(dtype=np.float32)
        y = group["label"].fillna(False).to_numpy(dtype=np.bool)
        n = len(x)

        if n < seq_len + 1:
            # Skip sequences that are too short to form at least one full window plus label
            continue

        # Ensure at least one full sequence in train
        split_idx = max(seq_len, int(n * (1 - validation_ratio)))

        # sliding window over the sequence dimension to create train and val sequences and labels
        for i in range(split_idx - seq_len + 1):
            train_seq.append(x[i: i + seq_len])
            train_labels.append(y[i + seq_len - 1])

        # sliding window for validation sequences, starting from split_idx to the end of the sequence
        for i in range(split_idx, n - seq_len + 1):
            val_seq.append(x[i: i + seq_len])
            val_labels.append(y[i + seq_len - 1])

    if not train_seq or not val_seq:
        raise ValueError(
            f"Not enough sequences for train/val splits. "
            f"Try reducing seq_len (current: {seq_len}) or validation_ratio."
        )

    return (
        np.stack(train_seq),
        np.stack(val_seq),
        np.array(train_labels, dtype=bool),
        np.array(val_labels, dtype=bool),
    )
