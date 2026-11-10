#!/usr/bin/env bash
# Применяет миграции Alembic, дождавшись доступности БД. Вызывается из entrypoint_api.sh.

set -e

# Параметры подключения (без пароля)
echo
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"
echo "POSTGRES_DB: $POSTGRES_DB"
echo "POSTGRES_USER: $POSTGRES_USER"

echo
echo "Waiting for database to start..."
timeout 30 bash -c \
  'while !</dev/tcp/$POSTGRES_HOST/$POSTGRES_PORT; do sleep 1; done'

echo
echo "Starting alembic migrations..."
python -m payment_service.launchers.alembic_runner upgrade head
