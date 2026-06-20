from ml.training.isolation_forest_config import TrainingConfig, seed_everything
from ml.training.isolation_forest_data import load_training_frame
from ml.training.isolation_forest_train import log_to_mlflow, train_isolation_forest


def main() -> int:
    cfg = TrainingConfig()
    seed_everything(cfg.seed)

    frame = load_training_frame(cfg)
    artifacts = train_isolation_forest(cfg, frame)
    run_id = log_to_mlflow(cfg, artifacts)

    print(
        f"Training complete. Model registered in MLflow with run ID: {run_id}")
    print(f"Registered model name: {cfg.mlflow_experiment_name}_model")
    print(
        f"Validation threshold score (lower is more anomalous): {artifacts.threshold:.6f}")

    for key, value in artifacts.metrics.items():
        print(f"{key}: {value:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
