from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from .config import APIConfig

# Define a security scheme for API key authentication
# The API key is expected to be provided in the "X-API-Key" header of the request.
_header_schema = APIKeyHeader(name="X-API-Key", auto_error=False)


def build_auth_dependency(cfg: APIConfig):
    """
    Build a FastAPI dependency that checks for a valid API key in the request headers.
    If the API key is missing or invalid, it raises an HTTP 403 Forbidden exception.
    This function returns a callable that can be used as a dependency in FastAPI routes.
    """
    async def _verify(key: str | None = Security(_header_schema)) -> None:
        if not key or key != cfg.api_key:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid or missing API key.",
            )

    return _verify
