#!/usr/bin/env bash
# Запуск консьюмера новых платежей (payments.new → шлюз). Миграции применяет api: compose
# запускает consumer после его healthcheck.

set -e

# exec — процесс становится PID 1 и получает SIGTERM от docker stop напрямую
exec python -m payment_service.launchers.consumer
