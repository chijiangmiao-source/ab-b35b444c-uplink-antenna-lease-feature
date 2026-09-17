#!/bin/sh
# Apply database migrations (PostgreSQL is the only clock) then serve the API.
set -euo pipefail

echo "[entrypoint] applying database migrations..."
alembic upgrade head

echo "[entrypoint] starting uvicorn..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
