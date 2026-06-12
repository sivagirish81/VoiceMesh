#!/usr/bin/env bash
set -euo pipefail

curl --fail --silent --show-error -X POST http://localhost:8000/demo/reset
echo

