"""
Stateless anomaly scoring functions.

Each scorer receives a loaded model snapshot and the feature data it needs,
runs inference, and returns a ScoredRow.  No I/O happens here — DB reads and
Kafka writes are the caller's concern.  This makes the scoring logic unit-
testable in complete isolation.

IForest path
------------
Scores a single feature window.  score_samples() returns a value where lower
means more anomalous; a score below the calibrated threshold is flagged.

LSTM-AE path
------------
Scores a sequence of consecutive windows for one entity.  The model was trained
on sequences of length seq_len, so exactly seq_len windows must be provided.
If the stream doesn't have that much history yet (early in its lifecycle), the
sequence is left-padded with zeros — the model interprets a silent past as
normal, which is a conservative choice that avoids false positives at startup.
Reconstruction error is the anomaly score: higher means more anomalous.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import numpy as np
import torch

from ml.training.features import flatten_feature_map
from services.scorer.model_registry import LoadedModel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScoredRow:
    """Result of scoring one feature window."""

    entity_id: str
    dataset: str
    stream_type: str
    window_end: datetime
    model_name: str
    model_version: int
    # raw model output (IForest: lower = worse; LSTM: higher = worse)
    anomaly_score: float
    is_anomaly: bool


def score_iforest(
    loaded: LoadedModel,
    entity_id: str,
    dataset: str,
    stream_type: str,
    window_end: datetime,
    feature_map: dict,
) -> ScoredRow:
    """
    Score a single feature window with the Isolation Forest pipeline.

    The pipeline includes a StandardScaler fitted during training, so raw
    feature values are passed in directly — no external scaling step needed.
    """
    feature_cols: list[str] = loaded.calibration["feature_columns"]
    threshold: float = loaded.calibration["threshold_score_samples"]

    flat = flatten_feature_map(feature_map)
    X = np.array(
        [[flat.get(col, 0.0) for col in feature_cols]], dtype=np.float64
    )

    raw_score = float(loaded.model.score_samples(X)[0])

    return ScoredRow(
        entity_id=entity_id,
        dataset=dataset,
        stream_type=stream_type,
        window_end=window_end,
        model_name=loaded.name,
        model_version=loaded.version,
        anomaly_score=raw_score,
        is_anomaly=raw_score < threshold,
    )


def score_lstm(
    loaded: LoadedModel,
    entity_id: str,
    dataset: str,
    stream_type: str,
    window_end: datetime,
    feature_rows: Sequence[dict],   # ordered oldest → newest, len ≤ seq_len
) -> ScoredRow:
    """
    Score a sequence of feature windows with the LSTM Autoencoder.

    feature_rows is the caller's responsibility to provide in ascending
    window_end order.  If fewer than seq_len rows are available, the sequence
    is left-padded with zero vectors so the model always receives a full-length
    input.  Zero-padding is semantically neutral: the model was trained on
    normal data, and a zero past looks like an un-exceptional stream history.
    """
    feature_cols: list[str] = loaded.calibration["feature_columns"]
    threshold: float = loaded.calibration["threshold_recon_error"]
    seq_len: int = loaded.calibration["seq_len"]
    n_features: int = loaded.calibration["n_features"]

    # Build a (len(feature_rows), n_features) matrix from the nested feature maps
    dense = np.zeros((len(feature_rows), n_features), dtype=np.float32)
    for i, fmap in enumerate(feature_rows):
        flat = flatten_feature_map(fmap)
        for j, col in enumerate(feature_cols):
            dense[i, j] = flat.get(col, 0.0)

    # Left-pad with zeros when fewer than seq_len windows are available
    if len(dense) < seq_len:
        pad = np.zeros((seq_len - len(dense), n_features), dtype=np.float32)
        dense = np.vstack([pad, dense])

    X = torch.tensor(dense).unsqueeze(0)  # (1, seq_len, n_features)

    with torch.no_grad():
        recon = loaded.model(X)
        error = float(torch.mean((X - recon) ** 2).item())

    return ScoredRow(
        entity_id=entity_id,
        dataset=dataset,
        stream_type=stream_type,
        window_end=window_end,
        model_name=loaded.name,
        model_version=loaded.version,
        anomaly_score=error,
        is_anomaly=error > threshold,
    )
