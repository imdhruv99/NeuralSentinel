from pydantic import Field
from pydantic_settings import BaseSettings

from config.settings import _MODEL_CONFIG, _PostgresSettings, _RedisSettings


class APIConfig(_PostgresSettings, _RedisSettings, BaseSettings):
    """
    Configuration for the alert API service. Inherits from _PostgresSettings and
    _RedisSettings to include database and cache connection settings.
    """
    model_config = _MODEL_CONFIG

    api_key: str = Field(alias="API_KEY")
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")

    # Pagination settings for API endpoints that return lists of items.
    default_page_size: int = Field(default=50, alias="API_DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=500, alias="API_MAX_PAGE_SIZE")

    # Polling interval for the server-sent events (SSE) endpoint. This controls how
    # frequently the server checks for new events to send to clients.
    sse_poll_interval_s: float = Field(
        default=2.0, alias="API_SSE_POLL_INTERVAL_S")
