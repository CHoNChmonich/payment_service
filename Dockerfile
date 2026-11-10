# Стадии: base → dependency-installer → builder (wheel проекта) → tests / production.
# Контекст сборки — корень проекта (см. docker-compose.yml и .dockerignore). Один образ на
# четыре процесса (api, relay, consumer, notifier): различаются entrypoint-скриптами.

# ====================================================
# Базовый образ: общие переменные среды, пользователь приложения. Entrypoint-скрипты копируются в
# конечных стадиях, чтобы их правка не сбрасывала кэш слоя зависимостей
FROM python:3.12-slim AS base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONDONTWRITEBYTECODE=1 \
    # uv: файлы копировать (кэш на другом слое), байт-код компилировать при установке,
    # Python брать из образа, кэш в слоях не хранить
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1 \
    # Окружение проекта — на PATH во всех стадиях
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system app \
 && useradd --system --gid app --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app


# ====================================================
# Зависимости приложения без dev-группы и без самого проекта: слой живёт, пока не меняются
# pyproject.toml и uv.lock
FROM base AS dependency-installer

COPY --from=ghcr.io/astral-sh/uv:0.7.15 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev


# ====================================================
# Сборка проекта: wheel собирается и ставится в то же окружение (не editable) — в production
# уходит только .venv с установленным пакетом, без исходников
FROM dependency-installer AS builder

COPY payment_service ./payment_service
RUN uv sync --frozen --no-dev --no-editable


# ====================================================
# Тесты: dev-зависимости, исходники и тесты. Unit-тесты выполняются при сборке образа,
# интеграционные — при запуске контейнера tests (entrypoint_tests.sh: отдельная БД и брокер стенда)
FROM dependency-installer AS tests

COPY payment_service ./payment_service
COPY tests ./tests
RUN uv sync --frozen \
 && pytest -q tests/payment_service/unit

COPY --chmod=755 deployment/entrypoints/entrypoint_*.sh /usr/bin/
ENTRYPOINT ["entrypoint_tests.sh"]


# ====================================================
# Production: только окружение с установленным пакетом — без uv, исходников и тестов; non-root.
# Entrypoint задаёт compose: entrypoint_api.sh / _relay.sh / _consumer.sh / _notifier.sh
FROM base AS production

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chmod=755 deployment/entrypoints/entrypoint_*.sh /usr/bin/
USER app

ENTRYPOINT ["entrypoint_api.sh"]
