import sys
import logging

_FORMAT = "%(asctime)s %(name)-40s %(levelname)-8s %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Noisy third-party loggers to silence below WARNING
_QUIET = [
    "mlflow",
    "urllib3",
    "botocore",
    "psycopg",
    "confluent_kafka",
    "httpx",
]


def setup_logging(level: str = "INFO") -> None:
    """
    Configure the root logger once at process start.
    Call this as the very first line of every service entrypoint (main.py)
    before any imports that might trigger library logging setup.

    Args:
        level: Root log level string ("DEBUG", "INFO", "WARNING", "ERROR").
               Can be driven from an env var: setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    Returns:
        None
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=_FORMAT,
        datefmt=_DATE_FORMAT,
        stream=sys.stdout,
        force=True,
    )
    for name in _QUIET:
        logging.getLogger(name).setLevel(logging.WARNING)
