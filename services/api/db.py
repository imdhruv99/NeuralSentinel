import asyncpg
from asyncpg import Pool
from datetime import datetime
from fastapi import Request


def get_pool(request: Request) -> Pool:
    """
    Retrieve the database connection pool from the FastAPI application state.
    This function can be used to access the connection pool in route handlers
    or other parts of the application.
    """
    return request.app.state.pool


async def fetch_alerts(
    pool: Pool,
    *,
    before: datetime | None,
    limit: int,
) -> list[asyncpg.Record]:
    """
    Fetches a list of alerts from the database.

    Args:
        pool (Pool): The asyncpg connection pool to use for database queries.
        before (str | None): Optional timestamp to filter alerts scored before this time.
        limit (int): The maximum number of alerts to fetch.

    Returns:
        list[asyncpg.Record]: A list of asyncpg.Record objects representing the fetched alerts.
    """
    if before:
        rows = await pool.fetch(
            """
            SELECT entity_id, window_end, model_name, model_version,
                anomaly_score, is_anomaly, scored_at
            FROM scores
            WHERE is_anomaly = true AND scored_at < $1
            ORDER BY scored_at DESC
            LIMIT $2
            """,
            before,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT entity_id, window_end, model_name, model_version,
                anomaly_score, is_anomaly, scored_at
            FROM scores
            WHERE is_anomaly = true
            ORDER BY scored_at DESC
            LIMIT $1
            """,
            limit,
        )
    return rows


async def fetch_alerts_since(
    pool: Pool,
    *,
    since: datetime,
    limit: int = 100,
) -> list[asyncpg.Record]:
    return await pool.fetch(
        """
        SELECT entity_id, window_end, model_name, model_version,
            anomaly_score, is_anomaly, scored_at
        FROM scores
        WHERE is_anomaly = true AND scored_at > $1
        ORDER BY scored_at ASC
        LIMIT $2
        """,
        since,
        limit,
    )


async def fetch_entity_series(
    pool: Pool,
    *,
    entity_id: str,
    before: datetime | None,
    limit: int,
) -> list[asyncpg.Record]:
    """
    Fetches a time series of scores for a specific entity from the database.
    Useful for generating time series plots for a given entity.

    Args:
        pool (Pool): The asyncpg connection pool to use for database queries.
        entity_id (str): The entity ID for which to fetch the time series.
        before (str | None): Optional timestamp to filter scores scored before this time.
        limit (int): The maximum number of scores to fetch.

    Returns:
        list[asyncpg.Record]: A list of asyncpg.Record objects representing the fetched scores for
        the specified entity.
    """
    if before:
        rows = await pool.fetch(
            """
            SELECT entity_id, window_end, model_name, model_version,
                anomaly_score, is_anomaly, scored_at
            FROM scores
            WHERE entity_id = $1 AND scored_at < $2
            ORDER BY scored_at DESC
            LIMIT $3
            """,
            entity_id,
            before,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT entity_id, window_end, model_name, model_version,
                   anomaly_score, is_anomaly, scored_at
            FROM scores
            WHERE entity_id = $1
            ORDER BY scored_at DESC
            LIMIT $2
            """,
            entity_id,
            limit,
        )
    return rows


async def fetch_current_models(pool: Pool) -> list[asyncpg.Record]:
    """
    Fetches the current models from the database.

    Args:
        pool (Pool): The asyncpg connection pool to use for database queries.

    Returns:
        list[asyncpg.Record]: A list of asyncpg.Record objects representing the current models.
    """
    return await pool.fetch(
        """
        SELECT DISTINCT ON (model_name)
            model_name, challenger_version AS version,
            decision, promoted_at
        FROM model_promotions
        WHERE decision IN ('PROMOTE' , 'NO_CHAMPION')
        ORDER BY model_name, promoted_at DESC
        """
    )


async def fetch_entity_ids(
    pool: Pool,
    *,
    dataset: str | None,
    limit: int,
) -> list[str]:
    """
    Fetches a list of distinct entity IDs from the database, optionally filtered by dataset.

    Args:
        pool (Pool): The asyncpg connection pool to use for database queries.
        dataset (str | None): Optional dataset prefix to filter entity IDs.
        limit (int): The maximum number of entity IDs to fetch.

    Returns:
        list[str]: A list of distinct entity IDs.
    """
    if dataset:
        rows = await pool.fetch(
            """
            SELECT DISTINCT entity_id
            FROM scores
            WHERE entity_id LIKE $1
            ORDER BY entity_id
            LIMIT $2
            """,
            f"{dataset}/%",
            limit,
        )
    else:
        rows = await pool.fetch(
            "SELECT DISTINCT entity_id FROM scores ORDER BY entity_id LIMIT $1",
            limit,
        )
    return [row["entity_id"] for row in rows]
