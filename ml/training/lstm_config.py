import random
import torch
import numpy as np

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from config.settings import (
    _MODEL_CONFIG,
    _PostgresSettings,
    _MLFlowSettings,
    _TrainingSettings,
    _seed_everything
)

seed_everything = _seed_everything


class LSTMAEConfig(_PostgresSettings, _MLFlowSettings, _TrainingSettings, BaseSettings):
    """
    LSTMAEConfig holds all the configuration for LSTM-AE training, including:
        - Postgres connection info for logging training metadata and model artifacts
        - MLflow tracking URI and experiment name
        - LSTM architecture hyperparameters (sequence length, hidden dimension, etc.)
        - Training hyperparameters (epochs, learning rate, batch size, early stopping patience)
        - Shared training controls (contamination, validation ratio, random seed)
        - Dataset and stream type to train on (e.g. "SMD/MULTIVARIATE")
    """
    model_config = _MODEL_CONFIG

    mlflow_experiment_name: str = Field(
        default="neural-sentinel-lstm-autoencoder", alias="LSTMAE_EXPERIMENT_NAME"
    )

    # Sequence architecture hyperparameters
    seq_len: int = Field(default=30, alias="SEQ_LEN")
    hidden_dim: int = Field(default=64, alias="HIDDEN_DIM")
    n_layers: int = Field(default=2, alias="N_LAYERS")
    dropout: float = Field(default=0.1, alias="DROPOUT")

    # Training hyperparameters
    epochs: int = Field(default=50, alias="EPOCHS")
    lr: float = Field(default=1e-3, alias="LR")
    batch_size: int = Field(default=256, alias="BATCH_SIZE")
    # Stop early if validation loss doesn't improve for this many consecutive epochs
    patience: int = Field(default=5, alias="EARLY_STOP_PATIENCE")

    # Shared training controls (same semantics as IForest)
    contamination: float = Field(default=0.05, alias="CONTAMINATION")
    validation_ratio: float = Field(default=0.2, alias="VALIDATION_RATIO")
    seed: int = Field(default=42, alias="TRAIN_SEED")

    # Dataset — prefixed to avoid collision with IForest's DATASET alias
    dataset: str = Field(default="SMD", alias="LSTMAE_DATASET")
    stream_type: str = Field(default="MULTIVARIATE",
                             alias="LSTMAE_STREAM_TYPE")
