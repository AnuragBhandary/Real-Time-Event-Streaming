"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import resources

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from livefeed import __version__
from livefeed.api.deps import AppState
from livefeed.api.routes import router
from livefeed.auth import Authenticator, create_api_key
from livefeed.bus import create_redis
from livefeed.config import Settings
from livefeed.db import create_pool
from livefeed.hub import Hub
from livefeed.ratelimit import RateLimiter


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(settings)
        redis = create_redis(settings.redis_url)
        if settings.bootstrap_api_key:
            await create_api_key(
                pool,
                "bootstrap",
                rate_per_s=settings.default_rate_per_s,
                burst=settings.default_burst,
                raw_key=settings.bootstrap_api_key,
            )
        hub = Hub(settings, pool, redis)
        await hub.start()
        app.state.livefeed = AppState(
            settings=settings,
            pool=pool,
            redis=redis,
            hub=hub,
            authenticator=Authenticator(pool, settings.auth_cache_ttl_s),
            limiter=RateLimiter(redis),
        )
        try:
            yield
        finally:
            await hub.stop()
            await redis.aclose()
            await pool.close()

    app = FastAPI(
        title="livefeed",
        version=__version__,
        summary="Real-time event streaming over WebSockets",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.get("/", include_in_schema=False)
    async def viewer() -> HTMLResponse:
        """A small live scoreboard page for demos."""
        page = resources.files("livefeed.static").joinpath("index.html").read_text()
        return HTMLResponse(page)

    @app.get("/healthz", tags=["ops"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["ops"])
    async def readyz(request: Request) -> Response:
        state: AppState = request.app.state.livefeed
        checks: dict[str, str] = {}
        try:
            await state.pool.fetchval("SELECT 1")
            checks["postgres"] = "ok"
        except Exception as exc:
            checks["postgres"] = f"error: {exc}"
        try:
            await state.redis.ping()
            checks["redis"] = "ok"
        except Exception as exc:
            checks["redis"] = f"error: {exc}"
        ok = all(v == "ok" for v in checks.values())
        return JSONResponse(checks, status_code=200 if ok else 503)

    @app.get("/v1/instance", tags=["ops"])
    async def instance(request: Request) -> dict[str, object]:
        """Which instance answered, and its local fan-out state (useful behind a balancer)."""
        state: AppState = request.app.state.livefeed
        return {
            "instance_id": settings.instance_id,
            "connections": len(state.hub.sessions),
            "channels": {k: len(v.sessions) for k, v in state.hub.channels.items()},
        }

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
