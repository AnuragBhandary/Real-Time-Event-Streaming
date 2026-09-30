"""Command line: ``livefeed {serve,migrate,create-key,simulate}``."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Sequence

from livefeed.config import Settings
from livefeed.logs import configure_logging


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="livefeed", description="Real-time event streaming")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run an instance (REST + WebSocket)")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)

    sub.add_parser("migrate", help="apply database migrations")

    key = sub.add_parser("create-key", help="create a producer API key and print it once")
    key.add_argument("--name", required=True)
    key.add_argument("--rate", type=float)
    key.add_argument("--burst", type=int)

    sim = sub.add_parser("simulate", help="publish simulated live matches")
    sim.add_argument("--url", default="http://localhost:8080")
    sim.add_argument("--api-key", default=os.environ.get("LIVEFEED_API_KEY"))
    sim.add_argument("--matches", type=int, default=3)
    sim.add_argument("--rate", type=float, default=5.0, help="events per second per match")
    sim.add_argument("--seed", type=int)
    return parser


async def _create_key(settings: Settings, name: str, rate: float, burst: int) -> str:
    from livefeed.auth import create_api_key
    from livefeed.db import create_pool

    pool = await create_pool(settings)
    try:
        _, raw = await create_api_key(pool, name, rate_per_s=rate, burst=burst)
        return raw
    finally:
        await pool.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level, settings.log_json)

    if args.command == "migrate":
        from livefeed.db import migrate

        applied = asyncio.run(migrate(settings.database_url))
        print(f"applied: {', '.join(applied) if applied else 'nothing (up to date)'}")
        return 0

    if args.command == "create-key":
        rate = args.rate or settings.default_rate_per_s
        burst = args.burst or settings.default_burst
        print(asyncio.run(_create_key(settings, args.name, rate, burst)))
        return 0

    if args.command == "simulate":
        from livefeed.simulator import simulate

        if not args.api_key:
            print("pass --api-key or set LIVEFEED_API_KEY", file=sys.stderr)
            return 2
        asyncio.run(simulate(args.url, args.api_key, args.matches, args.rate, args.seed))
        return 0

    if args.command == "serve":
        import uvicorn

        from livefeed.api.app import create_app

        uvicorn.run(
            create_app(settings),
            host=args.host or settings.host,
            port=args.port or settings.port,
            access_log=settings.access_log,
            log_config=None,
            ws_ping_interval=settings.ws_ping_interval_s,
            ws_ping_timeout=settings.ws_ping_timeout_s,
            timeout_graceful_shutdown=5,
        )
        return 0

    raise AssertionError(args.command)  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
