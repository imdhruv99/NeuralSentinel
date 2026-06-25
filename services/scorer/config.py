from pydantic import Field
from pydantic_settings import BaseSettings

from config.settings import (
    _MODEL_CONFIG,
    _KafkaSettings,
    _MLFlowSettings,
    _PostgresSettings,
    _RedisSettings,
)


class ScorerConfig(
    _PostgresSettings,
    _KafkaSettings,
    _RedisSettings,
    _MLFlowSettings,
    BaseSettings,
):
    """
    Configuration for the real-time anomaly scoring service.

    Inherits Postgres, Kafka, Redis, and MLflow connection settings from the
    shared base classes. All values are overridable via environment variables
    or the root .env file.
    """

    model_config = _MODEL_CONFIG

    # Poll loop
    # Sleep duration between poll cycles when the unscored-window query returns
    # no rows. Acts as the natural backpressure valve: if features arrive slowly
    # the scorer idles at this cadence rather than spinning.
    poll_interval_s: float = Field(
        default=5.0,
        alias="SCORER_POLL_INTERVAL_S",
    )

    # Maximum number of unscored windows to fetch and score per poll cycle.
    # Caps Postgres query result size and controls per-cycle latency.
    batch_size: int = Field(
        default=100,
        alias="SCORER_BATCH_SIZE",
    )

    # Re-check MLflow for Production version changes every N poll cycles.
    # At poll_interval_s=5s and model_refresh_cycles=12, the worst-case lag
    # between a promotion and the scorer picking up the new model is ~60s.
    model_refresh_cycles: int = Field(
        default=12,
        alias="SCORER_MODEL_REFRESH_CYCLES",
    )

    # Alerting
    # Suppress duplicate alerts for the same (model, entity) pair within this
    # window. Prevents alert storms when a stream stays anomalous for many
    # consecutive windows.
    alert_cooldown_s: int = Field(
        default=300,
        alias="ALERT_COOLDOWN_S",
    )

    # Model names — must match exactly what is registered in MLflow
    iforest_model_name: str = Field(
        default="neural-sentinel-isolation-forest-model",
        alias="IFOREST_MODEL_NAME",
    )
    lstm_model_name: str = Field(
        default="neural-sentinel-lstm-autoencoder-model",
        alias="LSTM_MODEL_NAME",
    )
