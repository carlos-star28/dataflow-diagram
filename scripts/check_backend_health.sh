#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:-http://127.0.0.1:8000}"
echo "[health] Checking ${API_URL}/api/import-status"
curl -fsS "${API_URL}/api/import-status" | head -c 400 && echo

echo "[health] Recent API logs:"
docker compose logs --tail=60 api
