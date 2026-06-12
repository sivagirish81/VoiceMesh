#!/usr/bin/env bash
set -euo pipefail

echo "Pausing Postgres for 15 seconds. Keep a browser call active to observe safe degradation."
docker compose pause postgres
trap 'docker compose unpause postgres >/dev/null 2>&1 || true' EXIT
sleep 15
docker compose unpause postgres
trap - EXIT
echo "Postgres resumed. Pool operations and the outbox publisher will recover on subsequent polls."

