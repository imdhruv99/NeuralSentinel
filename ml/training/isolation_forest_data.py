import json
import psycopg

import pandas as pd

from ml.training.features import flatten_feature_map
from ml.training.isolation_forest_config import TrainingConfig


def load_training_frame(cfg: TrainingConfig) -> pd.DataFrame:
    """
    Load the training data for the isolation forest model.
    """

    sql = """
        SELECT
            entity_id,
            dataset,
            stream_type,
            window_start,
            window_end,
            features,
            label
        FROM features
        WHERE dataset = %(dataset)s
        AND stream_type = %(stream_type)s
        ORDER BY window_end ASC
    """

    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {"dataset": cfg.dataset, "stream_type": cfg.stream_type},
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        raise ValueError(
            f"No training data found for dataset={cfg.dataset}, stream_type={cfg.stream_type}")

    flat_rows = []
    for _, r in df.iterrows():
        f_map = r["features"]
        if isinstance(f_map, str):
            # Convert JSON string to dict if necessary
            f_map = json.loads(f_map)
        flat = flatten_feature_map(f_map)
        flat["entity_id"] = r["entity_id"]
        flat["window_end"] = pd.to_datetime(r["window_end"], utc=True)
        flat["label"] = r["label"]
        flat_rows.append(flat)

    out = pd.DataFrame(flat_rows).sort_values(
        "window_end").reset_index(drop=True)
    return out
