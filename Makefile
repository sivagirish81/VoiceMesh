SHELL := /bin/bash
PYTEST ?= .venv/bin/pytest
RUFF ?= .venv/bin/ruff
MYPY ?= .venv/bin/mypy

.PHONY: up down restart logs api worker event-worker dashboard migrate create-topics \
	demo-normal-call demo-tts-backpressure demo-db-down demo-duplicate-events \
	demo-kill-worker demo-durable-action-cancel demo-billing-late-tts \
	demo-full-call-refund-trace demo-noise-vad demo-barge-in-confirmed \
	demo-barge-in-noise-rejected demo-barge-in-backchannel smoke-live-pipeline test lint

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

event-worker:
	python -m apps.worker.event_worker

dashboard:
	cd apps/dashboard && npm run dev

migrate:
	docker compose exec -T postgres psql -U postgres -d voice_lab -f /docker-entrypoint-initdb.d/001_init.sql

create-topics:
	python scripts/create_topics.py

demo-normal-call:
	python scripts/demo_normal_call.py

demo-tts-backpressure:
	docker compose exec -T api python scripts/demo_tts_backpressure.py

demo-db-down:
	./scripts/demo_db_down.sh

demo-duplicate-events:
	python scripts/demo_duplicate_events.py

demo-kill-worker:
	./scripts/kill_worker_demo.sh

demo-durable-action-cancel:
	docker compose exec -T api python scripts/demo_durable_action_cancel.py

demo-billing-late-tts:
	docker compose exec -T api python scripts/demo_billing_late_tts.py

demo-full-call-refund-trace:
	python scripts/demo_full_call_refund_cancel_trace.py

demo-noise-vad:
	python scripts/demo_noise_vad.py

demo-barge-in-confirmed:
	python scripts/demo_barge_in_confirmed.py

demo-barge-in-noise-rejected:
	python scripts/demo_barge_in_noise_rejected.py

demo-barge-in-backchannel:
	python scripts/demo_barge_in_backchannel.py

smoke-live-pipeline:
	docker compose exec -T api python scripts/smoke_live_pipeline.py

test:
	$(PYTEST) -q

lint:
	$(RUFF) check .
	$(MYPY) apps
	cd apps/dashboard && npm run lint
