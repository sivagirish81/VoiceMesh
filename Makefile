SHELL := /bin/bash

.PHONY: up down restart logs api worker dashboard migrate create-topics \
	demo-normal-call demo-tts-backpressure demo-db-down demo-duplicate-events \
	demo-kill-worker test lint

up:
	@test -f .env || (echo "Missing .env. Copy .env.example and set OPENAI_API_KEY." && exit 1)
	docker compose up --build -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=200

api:
	uvicorn apps.api.main:app --reload --host 0.0.0.0 --port 8000

worker:
	python -m apps.worker.temporal_worker

dashboard:
	cd apps/dashboard && npm run dev

migrate:
	docker compose exec -T postgres psql -U postgres -d voice_lab -f /docker-entrypoint-initdb.d/001_init.sql

create-topics:
	python scripts/create_topics.py

demo-normal-call:
	python scripts/demo_normal_call.py

demo-tts-backpressure:
	python scripts/demo_tts_backpressure.py

demo-db-down:
	./scripts/demo_db_down.sh

demo-duplicate-events:
	python scripts/demo_duplicate_events.py

demo-kill-worker:
	./scripts/kill_worker_demo.sh

test:
	pytest -q

lint:
	ruff check .
	mypy apps
	cd apps/dashboard && npm run lint

