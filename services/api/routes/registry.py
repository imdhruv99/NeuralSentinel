from asyncpg import Pool
from fastapi import APIRouter, Depends

from services.api.db import fetch_current_models, get_pool
from services.api.models import CurrentModel

router = APIRouter(prefix="/model", tags=["Model Registry"])


@router.get("/current", response_model=list[CurrentModel])
async def current_models(pool: Pool = Depends(get_pool)) -> list[CurrentModel]:
    """
    The currently active Production model version for each registered model.
    Returns one entry per model name. A dashboard can display this to show
    which model version is currently making scoring decisions.

    Args:
        pool (Pool): The asyncpg connection pool for the database, injected by FastAPI's
            dependency injection system.

    Returns:
        list[CurrentModel]: A list of Pydantic models representing the currently active
            production model versions for each registered model.
    """
    rows = await fetch_current_models(pool)
    return [CurrentModel.model_validate(dict(r)) for r in rows]
