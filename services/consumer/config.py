from pydantic import Field
from pydantic_settings import BaseSettings

from config.settings import _MODEL_CONFIG, _KafkaSettings, _PostgresSettings, _RedisSettings


class ConsumerConfig(_PostgresSettings, _KafkaSettings, _RedisSettings, BaseSettings):
    """
    Configuration for the feature-windowing consumer.
    """

    model_config = _MODEL_CONFIG

    consumer_group: str = Field(
        default="neural-sentinel-feature-windowing-consumer",
        alias="CONSUMER_GROUP",
        description="Kafka consumer group ID for the feature-windowing consumer.",
    )

    # Windowing Configuration

    # window_size_s is the amount of historical data the consumer
    # includes in each scored event's "metrics" payload.
    # The LSTM-AE model uses this window to compute reconstruction error
    # and generate anomaly scores. A longer window provides more context but
    # increases memory usage and may dilute the relevance of older data points.
    # A shorter window reduces memory usage and focuses on recent data but may
    # miss important patterns that develop over longer periods. The optimal value
    # depends on the specific characteristics of the dataset and the anomalies
    # being detected; 900 seconds (15 minutes) is a common starting point for time series
    # anomaly detection tasks.
    window_size_s: float = Field(
        default=900.0,
        alias="WINDOW_SIZE_S",
        description="Size of the feature window in seconds (e.g. 900s = 15 minutes).",
    )

    # slide_s is how far event-time advances between emitted feature rows.
    # slide_s == window_size_s -> tumbling (non-overlapping)
    # slide_s < window_size_s -> sliding (overlapping)
    # Controls the tradeoff between detection granularity and computational load.
    # Smaller slide_s means more frequent scoring and faster detection of anomalies,
    # but increases computational load and may produce more redundant data.
    slide_s: float = Field(
        default=900.0,
        alias="SLIDE_S",
        description=("Slide interval for the feature window in seconds. "
                     "Determines how often the consumer emits scored events. "
                     "A value equal to window_size_s means non-overlapping windows; "
                     "a smaller value means more frequent scoring with overlapping windows."),
    )

    # Batching/Loop Tuning
    # Persist + Commit once per batch. This is also the backpressure knob for the consumer:
    # if the producer is faster than the consumer, the consumer will
    # accumulate rows up to this limit before processing.
    batch_max_rows: int = Field(
        default=500,
        alias="CONSUMER_BATCH_MAX_ROWS",
        description="Maximum number of rows to process in a single batch.",
    )

    # Timeout for Kafka consumer polling. A longer timeout means the consumer waits
    # longer for new messages before processing a batch, which can increase latency
    # but reduce CPU usage when message frequency is low. A shorter timeout means the
    # consumer checks for new messages more frequently, which can reduce latency but
    # increase CPU usage when message frequency is low. 1000 ms (1 second) is a common
    # default that balances responsiveness with resource efficiency.
    poll_timeout_ms: int = Field(
        default=1000,
        alias="CONSUMER_POLL_TIMEOUT_MS",
        description="Timeout in milliseconds for Kafka consumer polling.",
    )
