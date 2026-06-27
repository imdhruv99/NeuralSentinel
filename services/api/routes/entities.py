from datetime import datetime
from typing import Optional

from asyncpg import Pool
from fastapi import APIRouter, Depends, Query

from services.api.db import fetch_entity_series, get_pool
from services.api.models import EntitySeries, ScoreRow

router = APIRouter(prefix="/entities", tags=["Entities"])


@router.get("/series", response_model=EntitySeries)
async def entity_series(
    entity_id: str = Query(
        ..., description="Full entity ID, e.g. NAB/realAdExchange/exchange-2_cpc_results"),
    before: Optional[datetime] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    pool: Pool = Depends(get_pool),
) -> EntitySeries:
    """
    All score rows (anomalous and normal) for a single entity, newest first.
    Use this to render a time-series chart showing anomaly score over time
    for a specific stream (e.g. a single NAB stream or SMD machine).

    Args:
        entity_id (str): The ID of the entity for which to fetch the score series.
        before (str | None): An optional ISO 8601 timestamp cursor. If provided,
            returns scores older than this value.
        limit (int): The maximum number of scores to return. Must be between 1 and
            500. Defaults to 50.
        pool (Pool): The asyncpg connection pool for the database, injected by FastAPI's
            dependency injection system.

    Returns:
        EntitySeries: A Pydantic model containing the paginated
            score series for the specified entity.
    """
    rows = await fetch_entity_series(pool, entity_id=entity_id, before=before, limit=limit)
    items = [ScoreRow.model_validate(dict(r)) for r in rows]
    next_cursor = items[-1].scored_at.isoformat() if len(items) == limit else None
    return EntitySeries(entity_id=entity_id, items=items, count=len(items), next_cursor=next_cursor)
