"""Сборка приложения FastAPI из готового контроллера."""

from typing import Any

from fastapi import FastAPI

from . import error_handlers
from .lifespan import Lifespan
from .v1 import PAYMENTS_TAG
from .v1 import Controller as V1Controller
from .v1 import create_router as create_v1_router

# Имя схемы безопасности OpenAPI: API-ключ в заголовке X-API-Key.
SECURITY_SCHEME = "apiKeyAuth"
API_KEY_HEADER = "X-API-Key"


class App(FastAPI):
    """FastAPI с двумя отличиями проекта в схеме OpenAPI: без 422 и со схемой API-ключа.

    FastAPI дописывает 422 каждой ручке с валидацией, но ошибки разбора запроса адаптер
    отдаёт как 400 (он уже объявлен в картах ответов роутера), поэтому 422 и его модели
    из схемы убираются; ``super().openapi()`` кэширует схему, правка в ней сохраняется.

    Требование ключа объявляется один раз на корне схемы: в схеме только защищённые операции
    (``healthcheck`` из неё исключён), а сам заголовок скрыт из параметров операций
    (``Header(include_in_schema=False)``) — Swagger UI отправляет его по схеме
    ``SECURITY_SCHEME``. Публичная операция в схеме должна будет снять требование через
    ``openapi_extra={"security": []}``.
    """

    def openapi(self) -> dict[str, Any]:
        """Возвращает схему OpenAPI без ответов 422, со схемой API-ключа и требованием ключа.

        Returns:
            Кэшированная схема OpenAPI приложения.
        """
        schema = super().openapi()
        for path in schema["paths"].values():
            for operation in path.values():
                operation.get("responses", {}).pop("422", None)
        for model in ("HTTPValidationError", "ValidationError"):
            schema.get("components", {}).get("schemas", {}).pop(model, None)
        schema.setdefault("components", {}).setdefault("securitySchemes", {})[SECURITY_SCHEME] = {
            "type": "apiKey",
            "in": "header",
            "name": API_KEY_HEADER,
        }
        schema["security"] = [{SECURITY_SCHEME: []}]
        return schema


def create_app(
    *,
    v1_controller: V1Controller,
    v1_prefix: str = "/api/v1",
    lifespan: Lifespan | None = None,
) -> FastAPI:
    """Собирает приложение: маршруты v1, обработчики ошибок, lifespan.

    CORS не настраивается: у сервиса нет браузерного фронтенда.

    Args:
        v1_controller: Контроллер v1 с уже внедрёнными портом сценариев и аутентификатором.
        v1_prefix: Префикс путей API v1.
        lifespan: Контекст жизненного цикла приложения (запуск и остановка адаптеров).

    Returns:
        Готовое к запуску приложение FastAPI.
    """
    app = App(
        title="Payment Service API",
        version="1.0.0",
        description="Асинхронная обработка платежей: приём запроса, обработка в фоне, "
        "уведомление webhook.",
        lifespan=lifespan,
        openapi_tags=[{"name": PAYMENTS_TAG}],
    )
    error_handlers.register_error_handlers(app)
    app.include_router(create_v1_router(v1_controller, prefix=v1_prefix))
    return app
