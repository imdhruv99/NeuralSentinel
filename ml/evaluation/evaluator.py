from dataclasses import dataclass, field
import json
import logging
import tempfile

import mlflow
import mlflow.artifacts
import mlflow.sklearn
import mlflow.tracking
import numpy as np
import pandas as pd
import psycopg
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score
import torch

from ml.evaluation.config import EvalConfig
from ml.training.features import flatten_feature_map

logger = logging.getLogger(__name__)


@dataclass
class EvalResult:
    precision: float
    recall: float
    f1: float
    pr_auc: float
    roc_auc: float
    n_samples: float
    anomaly_rate: float
    metrics: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.metrics = {
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
            "pr_auc": self.pr_auc,
            "roc_auc": self.roc_auc,
            "n_samples": float(self.n_samples),
            "anomaly_rate": self.anomaly_rate,
        }


def evaluate(model_name: str, version: int, cfg: EvalConfig) -> EvalResult:
    """
    Evaluate a model by loading it from MLflow, loading the calibration data and evaluation frame,
    and computing evaluation metrics.

    Args:
        - model_name: Name of the model to evaluate (e.g., "anomaly-iforest" or "anomaly-lstmae").
        - version: Version of the model to evaluate.
        - cfg: EvalConfig object containing evaluation parameters.

    Returns:
        - EvalResult object containing evaluation metrics.
    """
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)

    # Load the model and calibration data, then evaluate on the validation frame
    model_uri = f"models:/{model_name}/{version}"
    calib = _load_calibration(model_name, version, cfg)
    frame = _load_eval_frame(model_name, cfg)

    # Evaluate the model based on its type, else raise an error for unknown model names
    if model_name == "neural-sentinel-isolation-forest-model":
        return _eval_isolation_forest(model_uri, calib, frame)
    elif model_name == "neural-sentinel-lstm-autoencoder-model":
        return _eval_lstm_ae(model_uri, calib, frame)
    else:
        raise ValueError(f"Unknown model name: {model_name}")


def _load_calibration(model_name: str, version: int, cfg: EvalConfig) -> dict:
    """
    Load the calibration data for a given model from MLflow.

    Args:
        - model_name: Name of the model (e.g., "anomaly-iforest" or "anomaly-lstmae").
        - version: Version of the model.
        - cfg: EvalConfig object containing evaluation parameters.

    Returns:
        - A dictionary containing the calibration data.
    """

    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = mlflow.tracking.MlflowClient()

    mv = client.get_model_version(model_name, str(version))
    run_id = mv.run_id

    if model_name == "neural-sentinel-isolation-forest-model":
        artifact_path = "calibration/isolation_forest_threshold.json"
    else:
        artifact_path = "calibration/lstm_ae_threshold.json"

    with tempfile.TemporaryDirectory() as tmpdir:
        local = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path=artifact_path, dst_path=tmpdir
        )
        with open(local, "r") as f:
            return json.load(f)

    # Fallback in case of failure, though this should not happen
    return {"error": "Failed to load calibration data"}


def _load_eval_frame(model_name: str, cfg: EvalConfig) -> pd.DataFrame:
    """
    Query the last eval_validation_ratio fraction of rows for this model's
    dataset/stream_type from Postgres. Uses the same time-ordered split
    as training so the eval set was never seen by the model.

    Args:
        - model_name: Name of the model (e.g., "anomaly-iforest" or "anomaly-lstmae").
        - cfg: EvalConfig object containing evaluation parameters.

    Returns:
        - A pandas DataFrame containing the evaluation data.
    """

    if model_name == "neural-sentinel-isolation-forest-model":
        dataset, stream_type = "NAB", "UNIVARIATE"
    else:
        dataset, stream_type = "SMD", "MULTIVARIATE"

    sql = """
        SELECT entity_id, window_end, features, label
        FROM features
        WHERE dataset = %(dataset)s AND stream_type = %(stream_type)s
        ORDER BY window_end ASC
    """

    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"dataset": dataset, "stream_type": stream_type})
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        raise ValueError(f"No eval data found for {model_name}")

    # Take only the validation tail
    split_idx = int(len(df) * (1.0 - cfg.eval_validation_ratio))
    df = df.iloc[split_idx:].reset_index(drop=True)

    flat_rows = []
    for _, row in df.iterrows():
        f_map = row["features"]
        if isinstance(f_map, str):
            import json as _json
            f_map = _json.loads(f_map)
        flat = flatten_feature_map(f_map)
        flat["entity_id"] = row["entity_id"]
        flat["window_end"] = row["window_end"]
        flat["label"] = row["label"]
        flat_rows.append(flat)

    return pd.DataFrame(flat_rows)


def _eval_isolation_forest(model_uri: str, calib: dict, frame: pd.DataFrame) -> EvalResult:
    """
    Evaluate an Isolation Forest model on the provided evaluation frame using the calibration threshold.

    Args:
        - model_uri: URI of the Isolation Forest model in MLflow.
        - calib: Calibration data containing the threshold for anomaly detection.
        - frame: A pandas DataFrame containing the evaluation data, including a 'label' column.

    Returns:
        - EvalResult object containing evaluation metrics.
    """

    feature_cols = calib["feature_columns"]
    threshold = calib["threshold_score_samples"]

    model = mlflow.sklearn.load_model(model_uri)
    X = frame[feature_cols].fillna(0.0).values
    scores = model.score_samples(X)  # lower = more anomalous
    y_pred = scores < threshold
    y_true = frame["label"].fillna(False).astype(bool).values

    return _compute_metrics(y_true, y_pred, scores, invert_scores=True)


def _eval_lstm_ae(model_uri: str, calib: dict, frame: pd.DataFrame) -> EvalResult:
    """
    Evaluate an LSTM Autoencoder model on the provided evaluation frame using the calibration threshold.

    Args:
        - model_uri: URI of the LSTM Autoencoder model in MLflow.
        - calib: Calibration data containing the threshold for anomaly detection.
        - frame: A pandas DataFrame containing the evaluation data, including a 'label' column.

    Returns:
        - EvalResult object containing evaluation metrics.
    """

    from ml.training.lstm_data import make_train_val_sequences

    feature_cols = calib["feature_columns"]
    threshold = calib["threshold_recon_error"]
    seq_len = calib["seq_len"]

    model = mlflow.pytorch.load_model(model_uri)
    model.eval()

    # Build sequences the same way training did
    x_all = frame[feature_cols].fillna(0.0).values.astype(np.float32)
    y_all = frame["label"].fillna(False).astype(bool).values

    # Slide a window of seq_len over the rows to produce sequences
    sequences, labels = [], []
    for i in range(len(x_all) - seq_len + 1):
        sequences.append(x_all[i: i + seq_len])
        # label = last timestep in window
        labels.append(y_all[i + seq_len - 1])

    if not sequences:
        raise ValueError(
            f"Not enough rows to build sequences of length {seq_len}")

    X = torch.tensor(np.array(sequences))
    y_true = np.array(labels)

    with torch.no_grad():
        # (N, seq_len, features)
        recon = model(X)
        errors = torch.mean((X - recon) ** 2, dim=(1, 2)).numpy()  # (N,)

    y_pred = errors > threshold
    return _compute_metrics(y_true, y_pred, errors, invert_scores=False)


def _compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    scores: np.ndarray,
    invert_scores: bool,
) -> EvalResult:
    """
    Compute evaluation metrics for anomaly detection.

    Args:
        - y_true: Ground truth labels.
        - y_pred: Predicted labels.
        - scores: Anomaly scores.
        - invert_scores: Whether to invert scores (for "lower = more anomalous" models).

    Returns:
        - EvalResult object containing evaluation metrics.
    """
    # If scores are "lower = more anomalous", invert so sklearn's PR/ROC work
    # (they expect higher score = more anomalous)
    ranking_scores = -scores if invert_scores else scores

    precision = float(precision_score(y_true, y_pred, zero_division=0))
    recall = float(recall_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))

    if y_true.any() and (~y_true).any():
        pr_auc = float(average_precision_score(y_true, ranking_scores))
        roc_auc = float(roc_auc_score(y_true, ranking_scores))
    else:
        logger.warning(
            "eval set has no positive labels - PR-AUC and ROC-AUC set to 0")
        pr_auc = roc_auc = 0.0

    return EvalResult(
        precision=precision,
        recall=recall,
        f1=f1,
        pr_auc=pr_auc,
        roc_auc=roc_auc,
        n_samples=int(len(y_true)),
        anomaly_rate=float(np.mean(y_pred)),
    )
