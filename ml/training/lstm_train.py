import logging
import time
from dataclasses import dataclass

import mlflow
import numpy as np
import pandas as pd

import torch
from torch.utils.data import DataLoader, TensorDataset
import torch.nn.functional as F

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    average_precision_score,
    f1_score, precision_score,
    recall_score,
    roc_auc_score
)

from ml.training.lstm_config import LSTMAEConfig
from ml.training.lstm_data import make_train_val_sequences
from ml.training.lstm_model import LSTMAutoEncoder

logger = logging.getLogger(__name__)


def _pick_device() -> torch.device:
    """
    Utility function to pick the best available device (GPU > MPS > CPU) for training.
    This allows the training code to automatically leverage hardware acceleration when available, without hardcoding a specific device.
    """
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


@dataclass
class LSTMArtifacts:
    model: LSTMAutoEncoder
    scaler: StandardScaler
    threshold: float
    feature_columns: list[str]
    metrics: dict[str, float]
    n_features: int
    seq_len: int


def _run_epoch(
    model: LSTMAutoEncoder,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer | None,
    device: torch.device,
) -> float:
    """
    Run one epoch of training or evaluation.
    If optimizer is provided, runs in training mode with gradient updates.
    If optimizer is None, runs in evaluation mode without gradients.

    Args:
        - model: The LSTMAutoEncoder to train/evaluate
        - loader: DataLoader providing batches of input sequences
        - optimizer: The optimizer for training, or None for evaluation
        - device: The torch device to run on

    Returns:
        - The average loss over the epoch
    """
    is_train = optimizer is not None
    model.train(is_train)
    total_loss = 0.0

    # Use the appropriate gradient context: enable gradients for training, disable for evaluation
    grad_ctx = torch.enable_grad() if is_train else torch.no_grad()
    with grad_ctx:
        for (batch, ) in loader:
            batch = batch.to(device)
            recon = model(batch)
            loss = F.mse_loss(recon, batch)

            if is_train:
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            total_loss += loss.item() * len(batch)

    return total_loss / len(loader.dataset)


def train_lstm_ae(
    cfg: LSTMAEConfig,
    frame: pd.DataFrame,
) -> LSTMArtifacts:
    """
    Train an LSTM Autoencoder on the provided feature DataFrame according to the configuration in cfg.
    The training process includes:
        - Preparing the training and validation sequences from the flat feature DataFrame
        - Scaling the features using StandardScaler fitted on the training data
        - Training the LSTM Autoencoder with early stopping based on validation loss
        - Computing the reconstruction error on the validation set to determine the anomaly detection threshold
        - Returning the trained model, scaler, threshold, feature columns, and training metrics as artifacts

    Args:
        - cfg: LSTMAEConfig containing all the training configuration and hyperparameters
        - frame: A pandas DataFrame containing the preprocessed features, entity_id, window_end

    Returns:
        - LSTMArtifacts containing the trained model, scaler, threshold, feature columns, and metrics
    """
    device = _pick_device()
    logger.info("device=%s", device)

    feature_cols = [c for c in frame.columns if "__" in c]
    if not feature_cols:
        raise ValueError(
            "No feature columns found in the data frame. Expected columns with '__' in their names.")

    x_train, x_val, y_train, y_val = make_train_val_sequences(
        frame, feature_cols, cfg.seq_len, cfg.validation_ratio
    )
    logger.info(
        "sequences train=%d val=%d features=%d seq_len=%d",
        len(x_train), len(x_val), len(feature_cols), cfg.seq_len,
    )

    n_features = len(feature_cols)

    # Filter normal sequences BEFORE fitting the scaler so anomalous signals
    # don't skew the feature mean/variance the model trains against.
    normal_mask = ~y_train
    x_train_normal_raw = x_train[normal_mask]

    scaler = StandardScaler()
    scaler.fit(x_train_normal_raw.reshape(-1, n_features))

    # Transform all splits with the scaler fitted on normal sequences only.
    x_train_scaled = (
        scaler.transform(x_train.reshape(-1, n_features))
        .reshape(x_train.shape)
        .astype(np.float32)
    )
    x_val_scaled = (
        scaler.transform(x_val.reshape(-1, n_features))
        .reshape(x_val.shape)
        .astype(np.float32)
    )

    x_train_normal = x_train_scaled[normal_mask]
    logger.info(
        "normal training sequences: %d/%d", len(x_train_normal), len(x_train)
    )

    if len(x_train_normal) < cfg.batch_size:
        raise ValueError(
            f"Too few normal training sequences ({len(x_train_normal)}). "
            "Lower contamination or collect more training data."
        )

    # DataLoaders for training and validation.
    train_loader = DataLoader(
        TensorDataset(torch.tensor(x_train_normal)),
        batch_size=cfg.batch_size,
        shuffle=True,
    )
    val_loader = DataLoader(
        TensorDataset(torch.tensor(x_val_scaled)),
        batch_size=cfg.batch_size,
        shuffle=True,
    )

    # Initialize the LSTM Autoencoder model and optimizer.
    model = LSTMAutoEncoder(
        n_features=n_features,
        hidden_dim=cfg.hidden_dim,
        n_layers=cfg.n_layers,
        dropout=cfg.dropout,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr)

    best_val_loss = float("inf")
    best_state: dict = {}
    patience_counter = 0
    t0 = time.time()

    for epoch in range(1, cfg.epochs + 1):
        train_loss: float = _run_epoch(model, train_loader, optimizer, device)
        val_loss: float = _run_epoch(model, val_loader, None, device)
        elapsed: float = time.time() - t0

        logger.info(
            "epoch=%3d/%d train_loss=%.6f val_loss=%.6f elapsed=%6.1fs",
            epoch, cfg.epochs, train_loss, val_loss, elapsed,
        )

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            # clone the weights
            # checkpoint the best model weights so far to avoid referencing the same state_dict that gets updated in subsequent epochs
            best_state = {
                k: v.clone() for k, v in model.state_dict().items()
            }
        else:
            patience_counter += 1
            if patience_counter >= cfg.patience:
                logger.info(
                    "early stop at epoch %d (patience=%d)", epoch, cfg.patience
                )
                break

    # Restore the best model weights before returning the artifacts.
    model.load_state_dict(best_state)
    model.eval()

    # Compute reconstruction errors on the validation set to determine the anomaly detection threshold.
    val_errors: list = []
    with torch.no_grad():
        for (batch, ) in val_loader:
            val_errors.append(model.reconstruction_error(
                batch.to(device)).cpu().numpy())
    val_errors = np.concatenate(val_errors)

    # Threshold: flag the top `contamination` fraction of validation reconstruction errors as anomalies.
    # IForest used the bottom quantile of the anomaly scores as the threshold,
    # but for reconstruction error, higher is more anomalous, so we use the upper quantile.
    threshold = float(np.quantile(val_errors, 1.0 - cfg.contamination))
    logger.info(
        "threshold(recon_error @ q=%.3f)=%.6f", 1.0 -
        cfg.contamination, threshold
    )

    val_pred = val_errors > threshold
    metrics: dict[str, float] = {
        "val_best_loss": best_val_loss,
        "val_threshold_recon_error": threshold,
        "val_predicted_anomaly_rate": float(np.mean(val_pred)),
        "sequence_train": float(len(x_train)),
        "sequence_val": float(len(x_val)),
        "sequences_train_normal": float(len(x_train_normal)),
    }

    # Classification + Ranking metrics
    # Only where ground truth labels exist in the validation set (i.e. not all normal or all anomalous).
    if y_val.any() and (~y_val).any():
        y_true = y_val.astype(int)
        metrics["valid_roc_auc"] = float(roc_auc_score(y_true, val_errors))
        metrics["valid_pr_auc"] = float(
            average_precision_score(y_true, val_errors))
        metrics["valid_precision"] = float(
            precision_score(y_val, val_pred, zero_division=0))
        metrics["valid_recall"] = float(
            recall_score(y_val, val_pred, zero_division=0))
        metrics["valid_f1"] = float(f1_score(y_val, val_pred, zero_division=0))

    return LSTMArtifacts(
        model=model.cpu(),
        scaler=scaler,
        threshold=threshold,
        feature_columns=feature_cols,
        metrics=metrics,
        n_features=n_features,
        seq_len=cfg.seq_len,
    )


def log_to_mlflow(cfg: LSTMAEConfig, artifacts: LSTMArtifacts) -> str:
    """
    Logs the training configuration, metrics, and artifacts to MLflow.

    Args:
        cfg: LSTMAEConfig containing all the training configuration and hyperparameters
        artifacts: LSTMArtifacts containing the trained model, scaler, threshold, feature columns, and metrics

    Returns:
        The MLflow run ID as a string.
    """
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment_name)

    with mlflow.start_run(run_name=f"{cfg.mlflow_experiment_name}-baseline-train") as run:
        mlflow.log_params({
            "dataset":       cfg.dataset,
            "stream_type":   cfg.stream_type,
            "seq_len":       cfg.seq_len,
            "hidden_dim":    cfg.hidden_dim,
            "n_layers":      cfg.n_layers,
            "dropout":       cfg.dropout,
            "epochs":        cfg.epochs,
            "lr":            cfg.lr,
            "batch_size":    cfg.batch_size,
            "patience":      cfg.patience,
            "contamination": cfg.contamination,
            "seed":          cfg.seed,
            "n_features":    artifacts.n_features,
        })
        mlflow.log_metrics(artifacts.metrics)

        # Calibration artifact: threshold + feature schema needed at inference time
        mlflow.log_dict(
            {
                "threshold_recon_error": artifacts.threshold,
                "feature_columns": artifacts.feature_columns,
                "seq_len": artifacts.seq_len,
                "n_features": artifacts.n_features,
            },
            "calibration/lstm_ae_threshold.json",
        )

        # TorchScript export: serialises the model without needing the class
        # definition at load time, the scoring consumer can torch.jit.load()
        # it directly without importing LSTMAutoEncoder.
        example = torch.zeros(1, artifacts.seq_len, artifacts.n_features)
        scripted = torch.jit.trace(artifacts.model, example)
        mlflow.pytorch.log_model(
            scripted,
            artifact_path="model",
            registered_model_name="neural-sentinel-lstm-autoencoder-model",
            serialization_format="pickle",
        )
        logger.info("model registered: neural-sentinel-lstm-autoencoder-model")

    return run.info.run_id
