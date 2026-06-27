import asyncio
from datetime import datetime, timezone
from typing import Optional

from asyncpg import Pool
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from services.api.db import fetch_alerts, fetch_alerts_since, get_pool
from services.api.models import AlertPage, ScoreRow


router = APIRouter(prefix="/alerts", tags=["Alerts"])


def _build_page(rows, limit: int) -> AlertPage:
    """
    Build an AlertPage from a list of database rows.
    Each row is converted to a ScoreRow model, and the next_cursor is set to the
    window_end of the last item if the number of items equals the limit, otherwise None.

    Args:
        rows (list): A list of database rows representing alerts.
        limit (int): The maximum number of items to include in the page.

    Returns:
        AlertPage: A Pydantic model containing the paginated alerts.
    """
    items = [ScoreRow.model_validate(dict(r)) for r in rows]
    next_cursor = items[-1].scored_at.isoformat() if len(items) == limit else None
    return AlertPage(items=items, count=len(items), next_cursor=next_cursor)


@router.get("", response_model=AlertPage)
async def list_alerts(
    before: Optional[datetime] = Query(
        default=None,
        description="ISO-8601 timestamp cursor. Returns alerts older than this value.",
    ),
    limit: int = Query(default=50, ge=1, le=500),
    pool: Pool = Depends(get_pool),
) -> AlertPage:
    """
    List alerts with optional pagination.

    Args:
        before (str | None): An optional ISO 8601 timestamp cursor. If provided,
            returns alerts older than this value.
        limit (int): The maximum number of alerts to return. Must be between 1 and
            500. Defaults to 50.
        pool (Pool): The asyncpg connection pool for the database, injected by FastAPI's
            dependency injection system.

    Returns:
        AlertPage: A Pydantic model containing the paginated alerts.
    """
    rows = await fetch_alerts(pool=pool, before=before, limit=limit)
    return _build_page(rows, limit)


@router.get("/stream")
async def stream_alerts(
    pool: Pool = Depends(get_pool),
) -> StreamingResponse:
    """
    Stream alerts as Server-Sent Events (SSE).
    Only alerts that arrive after the connection is opened will be streamed.
    The stream runs until the client disconnects.

    Args:
        pool (Pool): The asyncpg connection pool for the database, injected by FastAPI's
            dependency injection system.

    Returns:
        StreamingResponse: A FastAPI StreamingResponse that streams alerts as SSE.
    """
    # Watermark: only stream alerts that arrive after the connection opens.
    # Use UTC explicitly; Postgres stores scored_at with timezone.
    # Keep as datetime, not a string. asyncpg requires Python datetime objects
    # for timestamptz bind parameters - passing a string raises DataError.
    watermark: datetime = datetime.now(timezone.utc)

    async def _generate():
        nonlocal watermark
        while True:
            rows = await fetch_alerts_since(pool, since=watermark, limit=100)
            for row in rows:
                scored_at: datetime = row["scored_at"]
                if scored_at > watermark:
                    watermark = scored_at
                payload = ScoreRow.model_validate(dict(row)).model_dump_json()
                yield f"data: {payload}\n\n"
            await asyncio.sleep(2.0)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # tells nginx not to buffer SSE
        },
    )
