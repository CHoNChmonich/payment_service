#!/usr/bin/env bash
# Прогон интеграционных тестов на отдельной БД (контейнер db_for_tests) и брокере стенда
# (временные очереди). Код выхода — код pytest: docker compose run --rm tests его возвращает.

set -e

# Параметры подключения (без пароля)
echo
echo "TEST_POSTGRES_HOST: $TEST_POSTGRES_HOST"
echo "TEST_POSTGRES_PORT: $TEST_POSTGRES_PORT"
echo "TEST_POSTGRES_DB: $TEST_POSTGRES_DB"
echo "TEST_POSTGRES_USER: $TEST_POSTGRES_USER"

echo
echo "Waiting for database to start..."
timeout 30 bash -c \
  'while !</dev/tcp/$TEST_POSTGRES_HOST/$TEST_POSTGRES_PORT; do sleep 1; done'

echo
echo "Running integration tests..."
exec pytest -q ./tests/payment_service/integration
