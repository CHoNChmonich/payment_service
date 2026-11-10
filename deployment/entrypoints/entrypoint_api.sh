#!/usr/bin/env bash
# Запуск API: миграции → uvicorn.
# Параметры из окружения: API_WORKER_COUNT, LOG_LEVEL.

set -e

/usr/bin/entrypoint_alembic.sh

: "${API_WORKER_COUNT:=2}"
: "${LOG_LEVEL:=INFO}"

echo
echo "API_WORKER_COUNT: ${API_WORKER_COUNT}"
echo "LOG_LEVEL: ${LOG_LEVEL}"
echo

# exec — uvicorn становится PID 1 и получает SIGTERM от docker stop напрямую
exec uvicorn payment_service.launchers.api:app \
  --host 0.0.0.0 --port 8000 \
  --workers "${API_WORKER_COUNT}" \
  --log-level "${LOG_LEVEL,,}"
