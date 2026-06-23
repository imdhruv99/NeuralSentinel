import json
import logging
import time

import numpy as np
import pandas as pd

from pathlib import Path

from dataclasses import dataclass

from sklearn.pipeline import Pipeline
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from mlflow.exceptions import MlflowException


from ml.training.isolation_forest_config import TrainingConfig

logger = logging.getLogger(__name__)


@dataclass
class TrainArtifacts:
    """
    Data class to hold the artifacts of the training process.
    """
    model: Pipeline
    threshold: float
    feature_columns: list[str]
    metrics: dict[str, float]
    validation_frame: pd.DataFrame


def _fit_iforest_with_progress(
    x_train_scaled: np.ndarray,
    cfg: TrainingConfig,
    progress_steps: int = 20,
) -> IsolationForest:
    """
    Fit IsolationForest incrementally with warm_start and print progress.

    Args:
        x_train_scaled: Scaled training data as a NumPy array.
        cfg: TrainingConfig object containing training parameters.
        progress_steps: Number of progress updates to print during training.

    Returns:
        Trained IsolationForest model.
    """
    total_trees = int(cfg.n_estimators)
    if total_trees <= 0:
        raise ValueError(f"n_estimators must be > 0, got {total_trees}")

    step = max(1, total_trees // progress_steps)
    checkpoints = list(range(step, total_trees, step)) + [total_trees]

    model = IsolationForest(
        n_estimators=checkpoints[0],
        contamination=cfg.contamination,
        random_state=cfg.seed,
        n_jobs=-1,
        warm_start=True,
    )

    t0 = time.time()
    for idx, n_trees in enumerate(checkpoints, start=1):
        model.set_params(n_estimators=n_trees)
        model.fit(x_train_scaled)

        pct = (n_trees / total_trees) * 100.0
        elapsed = time.time() - t0
        logger.info(
            "step=%d/%d trees=%d/%d progress=%6.2f%% elapsed=%7.2fs",
            idx, len(checkpoints), n_trees, total_trees, pct, elapsed,
        )

    return model


def train_isolation_forest(
    cfg: TrainingConfig,
    frame: pd.DataFrame
) -> TrainArtifacts:
    """
    Trains an Isolation Forest model on the provided DataFrame and evaluates it on a validation split.

    Args:
        cfg: TrainingConfig object containing training parameters.
        frame: A pandas DataFrame containing the training data, including a 'label' column for validation.

    Returns:
        TrainArtifacts object containing the trained model, threshold, feature columns, evaluation metrics, and validation
        DataFrame with scores and predictions.
    """

    if len(frame) < 200:
        raise ValueError(
            f"Need more rows for stable baseline training, got {len(frame)} rows. "
            "Try a larger dataset or a smaller validation ratio."
        )

    # Numeric feature columns produced by flattening: metric__stat
    feature_columns = [c for c in frame.columns if "__" in c]
    if not feature_columns:
        raise ValueError(
            "No feature columns found in training frame. "
            f"Expected columns with '__' in their names, got: {frame.columns.tolist()}"
        )

    x_all = frame[feature_columns].astype(float)
    if x_all.shape[1] == 0:
        raise ValueError("Feature matrix has zero columns after selection.")

    # Time-ordered split (prevents leakage from future into past)
    split_idx = int((1.0 - cfg.validation_ratio) * len(frame))
    split_idx = max(1, min(split_idx, len(frame) - 1))

    x_train = x_all.iloc[:split_idx]
    x_val = x_all.iloc[split_idx:].copy()
    valid = frame.iloc[split_idx:].copy()

    # Prefer fitting on known-normal rows when labels exist
    train_labels = frame.iloc[:split_idx]["label"]
    normal_mask = train_labels == False
    x_train_fit = x_train.loc[normal_mask] if normal_mask.any() else x_train

    if len(x_train_fit) == 0:
        raise ValueError("Training split is empty after normal-row filtering.")

    logger.info(
        "rows_total=%d rows_train=%d rows_valid=%d rows_train_fit=%d features=%d",
        len(frame), len(x_train), len(x_val), len(
            x_train_fit), len(feature_columns),
    )

    # Fit scaler once, then train IF with warm_start progress logging
    scaler = StandardScaler(with_mean=True, with_std=True)
    x_train_fit_scaled = scaler.fit_transform(x_train_fit.astype(float))
    x_val_scaled = scaler.transform(x_val.astype(float))

    iforest = _fit_iforest_with_progress(x_train_fit_scaled, cfg)

    # score_samples: lower = more anomalous
    valid_scores = iforest.score_samples(x_val_scaled)

    threshold = float(np.quantile(valid_scores, cfg.contamination))
    logger.info(
        "threshold(score_samples @ q=%s)=%.6f", cfg.contamination, threshold
    )

    valid_pred_anomaly = valid_scores <= threshold
    valid["score_samples"] = valid_scores
    valid["pred_anomaly"] = valid_pred_anomaly

    metrics: dict[str, float] = {
        "rows_total": float(len(frame)),
        "rows_train": float(len(x_train)),
        "rows_valid": float(len(x_val)),
        "rows_train_fit": float(len(x_train_fit)),
        "valid_threshold_score_samples": threshold,
        "valid_predicted_anomaly_rate": float(np.mean(valid_pred_anomaly)),
    }

    # Metrics only where ground-truth exists
    labeled_mask = valid["label"].notna()
    if labeled_mask.any():
        y_true = valid.loc[labeled_mask, "label"].astype(bool).to_numpy()
        y_pred = valid.loc[labeled_mask,
                           "pred_anomaly"].astype(bool).to_numpy()

        metrics["valid_precision"] = float(
            precision_score(y_true, y_pred, zero_division=0)
        )
        metrics["valid_recall"] = float(
            recall_score(y_true, y_pred, zero_division=0)
        )
        metrics["valid_f1"] = float(
            f1_score(y_true, y_pred, zero_division=0)
        )

        # For PR-AUC / ROC-AUC, larger score should mean "more anomalous"
        y_score = -valid.loc[labeled_mask, "score_samples"].to_numpy()
        if len(np.unique(y_true)) > 1:
            metrics["valid_pr_auc"] = float(
                average_precision_score(y_true, y_score)
            )
            metrics["valid_roc_auc"] = float(
                roc_auc_score(y_true, y_score)
            )

    # Build fitted pipeline object for downstream predict + mlflow.sklearn.log_model
    model = Pipeline(
        steps=[
            ("scaler", scaler),
            ("neural_sentinel_isolation_forest", iforest),
        ]
    )

    return TrainArtifacts(
        model=model,
        threshold=threshold,
        feature_columns=feature_columns,
        metrics=metrics,
        validation_frame=valid,
    )


def log_to_mlflow(cfg: TrainingConfig, artifacts: TrainArtifacts) -> str:
    """
    Logs the training artifacts and metrics to MLflow, and registers the model.

    Args:
        cfg: TrainingConfig object containing training parameters and MLflow configuration.
        artifacts: TrainArtifacts object containing the model, threshold, feature columns, metrics, and validation frame.

    Returns:
        The MLflow run ID of the logged training run.
    """

    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment_name)

    input_example = artifacts.validation_frame[artifacts.feature_columns].head(
        5)
    output_example = artifacts.model.predict(input_example)
    signature = infer_signature(input_example, output_example)

    with mlflow.start_run(run_name=f"{cfg.mlflow_experiment_name}-baseline-train") as run:
        mlflow.log_params(
            {
                "dataset": cfg.dataset,
                "stream_type": cfg.stream_type,
                "contamination": cfg.contamination,
                "n_estimators": cfg.n_estimators,
                "seed": cfg.seed,
                "validation_ratio": cfg.validation_ratio,
                "feature_count": len(artifacts.feature_columns),
            }
        )
        mlflow.log_metrics(artifacts.metrics)

        # Persist the model
        # Persist threshold and feature columns as artifacts for later use in inference
        payload = {
            "threshold_score_samples": artifacts.threshold,
            "feature_columns": artifacts.feature_columns,
        }

        Path("isolation_forest_threshold.json").write_text(
            json.dumps(payload, indent=2))
        mlflow.log_artifact("isolation_forest_threshold.json",
                            artifact_path="calibration")

        model_name = f"{cfg.mlflow_experiment_name}_model"
        try:
            mlflow.sklearn.log_model(
                sk_model=artifacts.model,
                artifact_path="model",
                signature=signature,
                input_example=input_example,
                registered_model_name=model_name,
            )
            logger.info("model registered: %s", model_name)

        except MlflowException as exc:
            logger.warning(
                "registration/model logging skipped due to server-client mismatch: %s", exc
            )
            mlflow.log_dict(
                {
                    "threshold_score_samples": artifacts.threshold,
                    "feature_columns": artifacts.feature_columns,
                    "model_name_expected": model_name,
                    "note": "Model registry/log_model skipped due to MLflow API mismatch",
                },
                "calibration/isolation_forest_threshold.json",
            )
            logger.info(
                "logged calibration fallback artifact; run marked successful")

        run_id = run.info.run_id
        return run_id
