"""Runtime configuration from ``LIVEFEED_*`` environment variables (or a ``.env`` file)."""

from __future__ import annotations

import socket

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LIVEFEED_", env_file=".env", extra="ignore")

    # --- storage -----------------------------------------------------------------------------
    database_url: str = "postgresql://livefeed:livefeed@localhost:5432/livefeed"
    redis_url: str = "redis://localhost:6379/0"
    db_pool_min: int = 2
    db_pool_max: int = 20

    # --- server ------------------------------------------------------------------------------
    host: str = "0.0.0.0"
    port: int = 8000
    instance_id: str = Field(default_factory=socket.gethostname)
    access_log: bool = False
    ws_ping_interval_s: float = 20.0
    ws_ping_timeout_s: float = 20.0

    # --- producers ---------------------------------------------------------------------------
    bootstrap_api_key: str | None = None
    auth_cache_ttl_s: float = 30.0
    default_rate_per_s: float = 1000.0
    default_burst: int = 2000
    max_batch: int = 500
    max_body_bytes: int = 1024 * 1024
    # Publishes allowed in flight per instance before new ones are shed with 503.
    max_inflight_publishes: int = 256

    # --- fan-out -----------------------------------------------------------------------------
    # Events buffered per WebSocket client; a client that falls this far behind is disconnected
    # with close code 4008 and resumes from its cursor (no data loss, bounded server memory).
    client_queue_max: int = 2000
    # Live events buffered for a client while its replay is still running.
    join_buffer_max: int = 10_000
    # A cursor further behind than this gets a snapshot instead of a replay.
    max_replay: int = 10_000
    replay_page: int = 500
    frame_max_events: int = 200
    heartbeat_interval_s: float = 15.0
    # How often each instance compares its channels with PostgreSQL to repair lost messages.
    resync_interval_s: float = 1.0
    channel_linger_s: float = 30.0
    channel_inbox_max: int = 10_000

    # --- observability -----------------------------------------------------------------------
    log_level: str = "INFO"
    log_json: bool = True
