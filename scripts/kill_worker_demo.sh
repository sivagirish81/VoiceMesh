#!/usr/bin/env bash
set -euo pipefail

echo "Stopping the Temporal worker. Temporal server and workflow history remain available."
docker compose stop temporal-worker
sleep 8
echo "Restarting the worker."
docker compose up -d temporal-worker
echo "Open http://localhost:8080 and inspect call-<call_id> to see workflow continuation."

