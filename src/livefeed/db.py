"""PostgreSQL connection pool and the SQL-file migration runner."""

from __future__ import annotations

import json
from importlib import resources

import asyncpg

from livefeed.config import Settings

# Arbitrary constant: serialises concurrent `livefeed migrate` runs (e.g. several containers).
_MIGRATION_LOCK_ID = 7_236_001


async def _init_connection(conn: asyncpg.Connection) -> None:
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )


async def create_pool(settings: Settings) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        init=_init_connection,
    )
    assert pool is not None
    return pool


def migration_files() -> list[tuple[str, str]]:
    """(version, sql) pairs shipped inside the package, in version order."""
    root = resources.files("livefeed.migrations")
    files = sorted(
        (entry for entry in root.iterdir() if entry.name.endswith(".sql")),
        key=lambda entry: entry.name,
    )
    return [(entry.name.removesuffix(".sql"), entry.read_text()) for entry in files]


async def migrate(dsn: str) -> list[str]:
    """Apply pending migrations, each in its own transaction. Returns the versions applied."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_ID)
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version TEXT PRIMARY KEY,"
                " applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            applied = {
                r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")
            }
            done: list[str] = []
            for version, sql in migration_files():
                if version in applied:
                    continue
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES ($1)", version
                    )
                done.append(version)
            return done
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_ID)
    finally:
        await conn.close()
