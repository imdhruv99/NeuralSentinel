import random
import numpy as np

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TrainingConfig(BaseSettings):
    """
    Configuration for training the Isolation Forest model.
    Loaded from environment variables or .env file.
    """
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    pg_host: str = Field(default="localhost", alias="PG_HOST")
    pg_port: int = Field(default=5432, alias="PG_PORT")
    pg_db: str = Field(default="projects", alias="POSTGRES_DB")
    pg_user: str = Field(default="nsapp", alias="PG_APP_USER")
    pg_password: str = Field(default="admin@123", alias="NSAPP_PASSWORD")

    mlflow_tracking_uri: str = Field(
        default="http://localhost:58083", alias="MLFLOW_TRACKING_URI"
    )
    mlflow_experiment_name: str = Field(
        default="neural-sentinel-isolation-forest", alias="MLFLOW_EXPERIMENT_NAME"
    )

    contamination: float = Field(default=0.05, alias="CONTAMINATION")
    n_estimators: int = Field(default=300, alias="N_ESTIMATORS")

    # Reproducibility: Seed for random number generators, and validation split ratio.
    seed: int = Field(default=42, alias="TRAIN_SEED")
    validation_ratio: float = Field(default=0.2, alias="VALIDATION_RATIO")

    # Dataset-specific: Which dataset to train on, and which stream type (NAB only).
    dataset: str = Field(default="NAB", alias="DATASET")
    stream_type: str = Field(default="UNIVARIATE", alias="IFOREST_STREAM_TYPE")

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} "
            f"port={self.pg_port} "
            f"dbname={self.pg_db} "
            f"user={self.pg_user} "
            f"password={self.pg_password}"
        )


def seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
