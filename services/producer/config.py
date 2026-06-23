from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

from config.settings import _MODEL_CONFIG, _KafkaSettings


class ProducerConfig(_KafkaSettings, BaseSettings):
    """
    Producer configuration. Kafka connection + topic fields come from _KafkaSettings.
    Only producer-specific fields are defined here.
    """

    model_config = _MODEL_CONFIG

    # NAB producer settings
    nab_data_dir: Path = Field(
        default=Path("data/NAB/data"),
        alias="NAB_DATA_DIR",
        description="Path to the NAB data directory (Contains subdirs per category).",
    )
    nab_labels_file: Path = Field(
        default=Path("data/NAB/labels/combined_labels.json"),
        alias="NAB_LABELS_FILE",
        description="Path to the combined_labels.json ground-truth file for NAB dataset.",
    )
    nab_replay_speed: float = Field(
        default=0.0,
        alias="NAB_REPLAY_SPEED",
        description=(
            "Replay speed multiplier for NAB data."
            "0.0 means max speed (no sleep), 1.0 means real-time"
            "0.1, means 10x faster than real-time, etc."
        )
    )
    # Optionally limit to a single NAB stream for quick testing.
    # e.g. "realAWSCloudwatch/cpu_24hr_activate.csv"
    nab_stream_filter: str | None = Field(
        default=None,
        alias="NAB_STREAM_FILTER",
        description="If set, only replay the stream whose relative path contains this string.",
    )

    # SMD Producer settings
    smd_data_dir: Path = Field(
        default=Path("data/SMD/ServerMachineDataset"),
        alias="SMD_DATA_DIR",
        description=(
            "Path to the SMD ServerMachineDataset directory. "
            "Expected layout: train/, test/, test_label/ subdirectories."
        ),
    )
    smd_replay_speed: float = Field(
        default=0.0,
        alias="SMD_REPLAY_SPEED",
        description=(
            "Replay speed multiplier for SMD data."
            "0.0 means max speed (no sleep), 1.0 means real-time"
            "0.1, means 10x faster than real-time, etc."
        )
    )
    # SMD has 28 machines. Filter to a subset for quick testing.
    # e.g. "machine-1-1" to replay only that machine.
    smd_machine_filter: str | None = Field(
        default=None,
        alias="SMD_MACHINE_FILTER",
        description="If set, only replay data from the machine whose ID contains this string.",
    )
    # Whether to use the test split (with anomaly labels) or train split.
    # Train split has no labels — is_anomaly will be None for all events.
    # Test split has labels in test_label/ — is_anomaly will be set.
    smd_use_test_split: bool = Field(
        default=True,
        alias="SMD_USE_TEST_SPLIT",
        description="Whether to use the test split (with anomaly labels) or train split.",
    )

    # Kafka producer performance settings
    producer_batch_size: int = Field(
        default=16384,  # 16KB - Kafka's default
        alias="PRODUCER_BATCH_SIZE",
        description=(
            "Kafka producer batch size in bytes. The producer buffers records "
            "and sends them in batches. Larger batches = higher throughput but "
            "more memory and slightly higher latency. 16 KB is a good default "
            "for our message sizes (~1-2 KB each)."
        )
    )
    producer_linger_ms: int = Field(
        default=5,
        alias="PRODUCER_LINGER_MS",
        description=(
            "How long (ms) the producer waits to fill a batch before sending. "
            "0 = send immediately (lowest latency, lower throughput). "
            "5ms gives a small window to accumulate messages into a batch "
            "without noticeable latency impact for our replay use case."
        ),
    )
    producer_compression: str = Field(
        default="gzip",
        alias="PRODUCER_COMPRESSION",
        description=(
            "Compression codec: none, gzip, snappy, lz4, zstd. "
            "gzip has the best compression ratio; snappy/lz4 are faster to "
            "compress/decompress. Our JSON messages compress well (~4-6x), "
            "so gzip is a good default."
        ),
    )

    @field_validator("producer_compression")
    @classmethod
    def validate_compression(cls, v: str) -> str:
        allowed = {"none", "gzip", "snappy", "lz4", "zstd"}
        if v not in allowed:
            raise ValueError(
                f"producer_compression must be one of {allowed}, got '{v}'")
        return v
