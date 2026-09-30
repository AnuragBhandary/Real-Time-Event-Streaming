.PHONY: install infra up down test lint typecheck check demo load-smoke load

install:            ## install dependencies (needs uv)
	uv sync

infra:              ## start only PostgreSQL and Redis (for tests)
	docker compose up -d --wait postgres redis

up:                 ## 3 instances behind nginx on :8080
	docker compose up -d --build --wait

down:
	docker compose down

test: infra
	uv run pytest --cov --cov-fail-under=90

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test

demo: up           ## simulated matches; watch them at http://localhost:8080
	uv run livefeed simulate --url http://localhost:8080 \
		--api-key lf_0000000000de_local-dev-key-not-for-production --matches 3 --rate 5

load-smoke: up     ## ~1 minute
	uv run python bench/loadtest.py --clients 60 --streams 3 --events 3000 --rate 300 \
		--subscriber-procs 2 --drop-mean-s 3 --kill-interval 4 --pubsub-kill-interval 3 --label smoke

load: up           ## the resume numbers (about 5 minutes)
	uv run python bench/loadtest.py --in-docker --clients 1000 --streams 20 --events 100000 \
		--rate 400 --subscriber-procs 4 --drop-mean-s 20 --kill-interval 45 \
		--pubsub-kill-interval 30 --label full-1000-clients-chaos
