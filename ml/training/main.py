import argparse

from ml.training.isolation_forest_config import TrainingConfig, seed_everything
from ml.training.isolation_forest_data import load_training_frame
from ml.training.isolation_forest_train import log_to_mlflow, train_isolation_forest

from ml.training.lstm_config import LSTMAEConfig, seed_everything
from ml.training.lstm_data import load_sequence_frame
from ml.training.lstm_train import (
    log_to_mlflow as lstm_log,
    train_lstm_ae,
)


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

    print(f"\nTraining complete. Run ID: {run_id}")
    print("Registered model: anomaly-iforest")
    print(
        f"Threshold (score_samples, lower = more anomalous): {artifacts.threshold:.6f}")
    for key, value in artifacts.metrics.items():
        print(f"  {key}: {value:.6f}")
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

    print(f"\nTraining complete. Run ID: {run_id}")
    print("Registered model: anomaly-lstmae")
    print(
        f"Threshold (recon error, higher = more anomalous): {artifacts.threshold:.6f}")
    for key, value in artifacts.metrics.items():
        print(f"  {key}: {value:.4f}")
    return 0


def main() -> int:
    """
    Main entrypoint for training models.
    Usage: `python -m ml.training.main <model>`
    where <model> is either "iforest" or "lstm-ae".
    """
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
