import logging
from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.logging import setup_logging
from services.api.auth import build_auth_dependency
from services.api.config import APIConfig
from services.api.routes import alerts, entities, health, registry

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    setup_logging()
    cfg = APIConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("connecting to postgres pool",
                    extra={"dsn_host": cfg.pg_host})
        # Pass credentials as keyword arguments rather than a URL DSN.
        # A URL DSN requires percent-encoding any special characters in the
        # password; keyword args are treated as raw strings by asyncpg, so
        # characters like @ or $ in the password can never break parsing.
        app.state.pool = await asyncpg.create_pool(
            host=cfg.pg_host,
            port=cfg.pg_port,
            database=cfg.pg_db,
            user=cfg.pg_user,
            password=cfg.pg_password,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        logger.info("postgres pool ready")
        yield
        await app.state.pool.close()
        logger.info("postgres pool closed")

    app = FastAPI(
        title="NeuralSentinel API",
        description=(
            "Query interface for real-time anomaly scores and alerts "
            "produced by the NeuralSentinel scoring pipeline."
        ),
        version="1.0.0",
        lifespan=lifespan,
        # OpenAPI is served at /docs (Swagger UI) and /redoc.
        # Require auth on the OpenAPI JSON too if you want it private:
        # openapi_url=None disables the schema endpoint entirely.
    )

    # CORS: allow all origins for development. Lock this down in production
    # to the specific dashboard origin(s).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # Build the auth dependency once, bound to this config instance.
    require_auth = build_auth_dependency(cfg)

    # Health has no auth; load balancers probe it before routing traffic.
    app.include_router(health.router)

    # All operational routes require the API key.
    app.include_router(alerts.router, dependencies=[Depends(require_auth)])
    app.include_router(entities.router, dependencies=[Depends(require_auth)])
    app.include_router(registry.router, dependencies=[Depends(require_auth)])

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    cfg = APIConfig()
    uvicorn.run(
        "services.api.main:app",
        host=cfg.api_host,
        port=cfg.api_port,
        reload=False,
        log_config=None,   # suppress uvicorn's default logging;
    )
