#!/bin/bash
set -e
exec uvicorn backend.main:app --host 0.0.0.0 --port "${APP_PORT:-8000}"
