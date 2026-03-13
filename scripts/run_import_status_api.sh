#!/usr/bin/env bash
set -euo pipefail

"/Volumes/Data/VS Code/Dataflow Digram/.venv/bin/uvicorn" backend.import_status_api:app --host 0.0.0.0 --port 8000 --reload
