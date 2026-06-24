import random
import numpy as np

from pydantic import Field
from pydantic_settings import BaseSettings

from config.settings import (
    _MODEL_CONFIG,
    _MLFlowSettings,
    _PostgresSettings,
    _TrainingSettings,
    _seed_everything
)

seed_everything = _seed_everything


class TrainingConfig(_PostgresSettings, _MLFlowSettings, _TrainingSettings, BaseSettings):
    """
    Configuration for training the Isolation Forest model.
    Loaded from environment variables or .env file.
    """
    model_config = _MODEL_CONFIG

    mlflow_experiment_name: str = Field(
        default="neural-sentinel-isolation-forest", alias="IFOREST_EXPERIMENT_NAME"
    )

    contamination: float = Field(default=0.05, alias="CONTAMINATION")
    n_estimators: int = Field(default=300, alias="N_ESTIMATORS")

    # Reproducibility: Seed for random number generators, and validation split ratio.
    seed: int = Field(default=42, alias="TRAIN_SEED")
    validation_ratio: float = Field(default=0.2, alias="VALIDATION_RATIO")

    # Dataset-specific: Which dataset to train on, and which stream type (NAB only).
    dataset: str = Field(default="NAB", alias="DATASET")
    stream_type: str = Field(default="UNIVARIATE", alias="IFOREST_STREAM_TYPE")
