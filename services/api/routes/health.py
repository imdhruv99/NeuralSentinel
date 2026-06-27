from asyncpg import Pool
from fastapi import APIRouter, Depends

from services.api.db import get_pool
from services.api.models import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/healthz", response_model=HealthResponse, include_in_schema=True)
async def healthz(pool: Pool = Depends(get_pool)):
    """
    Health check endpoint for the service.
    This endpoint checks the health of the service and its database connection.
    It returns a JSON response indicating the health status of the service and the database connection.

    Args:
        pool (Pool): The asyncpg connection pool for the database, injected by FastAPI's
        dependency injection system.

    Returns:
        HealthResponse: A Pydantic model containing the health status of the service and the database
        connection.
    """
    try:
        await pool.fetchval("SELECT 1")
        db_status = "ok"
    except Exception:
        db_status = "unreachable"

    return HealthResponse(status="ok", db=db_status)
