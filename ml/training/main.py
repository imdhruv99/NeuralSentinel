import argparse
import logging

from config.logging import setup_logging
from config.settings import _seed_everything as seed_everything
from ml.training.isolation_forest_config import TrainingConfig
from ml.training.isolation_forest_data import load_training_frame
from ml.training.isolation_forest_train import log_to_mlflow, train_isolation_forest

from ml.training.lstm_config import LSTMAEConfig
from ml.training.lstm_data import load_sequence_frame
from ml.training.lstm_train import (
    log_to_mlflow as lstm_log,
    train_lstm_ae,
)

logger = logging.getLogger(__name__)


def run_isolation_forest() -> int:
    """
    Run the isolation forest training pipeline:
        - Load training data from Postgres
        - Train the isolation forest model and compute metrics/artifacts
        - Log the model and artifacts to MLflow and register the model

    Returns:
        - Exit code (0 for success)
    """
    cfg = TrainingConfig()
    seed_everything(cfg.seed)

    frame = load_training_frame(cfg)
    artifacts = train_isolation_forest(cfg, frame)
    run_id = log_to_mlflow(cfg, artifacts)

    logger.info("training complete — run_id=%s", run_id)
    logger.info("registered model: anomaly-iforest")
    logger.info(
        "threshold (score_samples, lower = more anomalous): %.6f", artifacts.threshold)
    for key, value in artifacts.metrics.items():
        logger.info("  %s: %.6f", key, value)
    return 0


def run_lstm_ae() -> int:
    """
    Run the LSTM-AE training pipeline:
        - Load preprocessed sequence data from Postgres into a DataFrame
        - Train the LSTM-AE model and compute metrics/artifacts
        - Log the model and artifacts to MLflow and register the model

    Returns:
        - Exit code (0 for success)
    """
    cfg = LSTMAEConfig()
    seed_everything(cfg.seed)

    frame = load_sequence_frame(cfg)
    artifacts = train_lstm_ae(cfg, frame)
    run_id = lstm_log(cfg, artifacts)

    logger.info("training complete — run_id=%s", run_id)
    logger.info("registered model: anomaly-lstmae")
    logger.info(
        "threshold (recon error, higher = more anomalous): %.6f", artifacts.threshold)
    for key, value in artifacts.metrics.items():
        logger.info("  %s: %.4f", key, value)
    return 0


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(
        prog="python -m ml.training.main",
        description="NeuralSentinel model training entrypoint.",
    )
    parser.add_argument(
        "model",
        choices=["iforest", "lstm-ae"],
        help=(
            "iforest  - Isolation Forest on NAB UNIVARIATE features. "
            "lstm-ae  - LSTM Autoencoder on SMD MULTIVARIATE sequences."
        ),
    )
    args = parser.parse_args()

    if args.model == "iforest":
        return run_isolation_forest()
    return run_lstm_ae()


if __name__ == "__main__":
    raise SystemExit(main())
