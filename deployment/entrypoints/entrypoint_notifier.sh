#!/usr/bin/env bash
# Запуск консьюмера уведомлений (payments.processed → webhook). Миграции применяет api: compose
# запускает notifier после его healthcheck.

set -e

# exec — процесс становится PID 1 и получает SIGTERM от docker stop напрямую
exec python -m payment_service.launchers.notifier
