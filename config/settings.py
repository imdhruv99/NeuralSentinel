import logging
import random
from pathlib import Path

import numpy as np
from pydantic import BaseModel, Field
from pydantic_settings import SettingsConfigDict

import torch

logger = logging.getLogger(__name__)

_ENV_FILE = Path(__file__).parent.parent / ".env"

_MODEL_CONFIG = SettingsConfigDict(
    env_file=_ENV_FILE,
    env_file_encoding="utf-8",
    extra="ignore",
)


class _PostgresSettings(BaseModel):
    """
    Postgres connection settings. The producer doesn't use Postgres, but the
    consumer does, so I put these here in the shared config to keep all settings in
    one place. The consumer's FeatureSink uses these to connect and write batches
    of features and labels to Postgres after windowing.
    """
    pg_host: str = Field(default="localhost", alias="PG_HOST")
    pg_port: int = Field(default=5432, alias="PG_PORT")
    pg_db: str = Field(default="postgres", alias="POSTGRES_DB")
    pg_user: str = Field(default="nsapp", alias="PG_APP_USER")
    pg_password: str = Field(default="admin@123", alias="NSAPP_PASSWORD")

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} "
            f"dbname={self.pg_db} user={self.pg_user} password={self.pg_password}"
        )


class _KafkaSettings(BaseModel):
    """
    Kafka connection settings. Both producer and consumer use these to connect to
    Kafka and read/write events.raw.
    """
    kafka_bootstrap_servers: str = Field(
        default="localhost:19091,localhost:19092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    topic_events_raw: str = Field(default="events.raw", alias="EVENTS_TOPIC")
    topic_events_scored: str = Field(
        default="events.scored", alias="SCORED_TOPIC")
    topic_alerts: str = Field(default="alerts", alias="ALERTS_TOPIC")


class _MLFlowSettings(BaseModel):
    """
    MLFlow tracking settings. The producer doesn't use MLFlow, but the consumer
    does for model performance tracking, so I put these here in the shared config.
    The consumer's scoring loop uses these to log metrics and parameters to MLFlow.
    """
    mlflow_tracking_uri: str = Field(
        default="http://localhost:58083", alias="MLFLOW_TRACKING_URI"
    )


class _TrainingSettings(BaseModel):
    """
    Hyperparameters that are identical across all model training jobs:
    contamination rate, validation split, and random seed.
    Adding a new model type inherits these for free.
    """
    contamination: float = Field(default=0.05, alias="CONTAMINATION")
    validation_ratio: float = Field(default=0.2, alias="VALIDATION_RATIO")
    seed: int = Field(default=42, alias="TRAIN_SEED")


class _RedisSettings(BaseModel):
    """
    Redis connection settings. The consumer uses Redis to cache the LSTM-AE's
    sequence buffer and to store the latest anomaly score per stream for alerting.
    """
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="admin@123", alias="REDIS_PASSWORD")
    redis_db: int = Field(default=0, alias="REDIS_DB")


def _seed_everything(seed: int) -> None:
    """
    Seed all RNGs that could affect training reproducibility.
    torch.manual_seed is a no-op cost when torch is installed but not used,
    so one function covers both IForest and LSTM-AE safely.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    logger.debug("seeded random, numpy, torch with seed=%d", seed)
