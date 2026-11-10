#!/usr/bin/env bash
# Запуск процесса доставки событий outbox в RabbitMQ. Миграции применяет api: compose
# запускает relay после его healthcheck.

set -e

# exec — процесс становится PID 1 и получает SIGTERM от docker stop напрямую
exec python -m payment_service.launchers.relay
