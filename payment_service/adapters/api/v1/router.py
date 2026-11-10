"""Маршруты v1 объявляются императивно — так маршрутизация остаётся вне контроллера.

Этот модуль и есть рукописный контракт API: пути, ``operation_id``, теги и карты ответов.
"""

from typing import Any

from fastapi import APIRouter

from payment_service.adapters.api.dtos import ErrorResponse
from payment_service.adapters.api.v1.controller import Controller

# Путь служебной проверки под префиксом v1.
HEALTHCHECK_PATH = "/healthcheck"
PAYMENTS_TAG = "v1: Payments"


def create_router(controller: Controller, *, prefix: str = "/api/v1") -> APIRouter:
    """Объявляет маршруты v1 поверх методов контроллера.

    Args:
        controller: Контроллер, методы которого становятся обработчиками.
        prefix: Префикс путей маршрутизатора.

    Returns:
        Маршрутизатор с путями, ``operation_id``, тегами и картами ответов.
    """
    busy_response: dict[str, Any] = {
        "model": ErrorResponse,
        "description": "Хранилище временно недоступно; повторите запрос",
        "headers": {
            "Retry-After": {
                "description": "Через сколько секунд повторить запрос.",
                "schema": {"type": "integer", "minimum": 1},
            }
        },
    }
    unauthorized_response: dict[str, Any] = {
        "model": ErrorResponse,
        "description": "Нет API-ключа или он недействителен",
        "headers": {
            "WWW-Authenticate": {
                "description": "Схема аутентификации: API-ключ в заголовке X-API-Key.",
                "schema": {"type": "string"},
            }
        },
    }
    default_responses: dict[int | str, dict[str, Any]] = {
        400: {"model": ErrorResponse},
        401: unauthorized_response,
        500: {"model": ErrorResponse},
        503: busy_response,
    }
    not_found_responses: dict[int | str, dict[str, Any]] = {
        **default_responses,
        404: {"model": ErrorResponse},
    }
    conflict_responses: dict[int | str, dict[str, Any]] = {
        **default_responses,
        409: {
            "model": ErrorResponse,
            "description": "Под этим Idempotency-Key уже создан платёж с другими данными",
        },
    }

    router = APIRouter(prefix=prefix)

    router.add_api_route(
        "/payments",
        controller.create_payment,
        methods=["POST"],
        tags=[PAYMENTS_TAG],
        summary="Принять платёж в обработку (идемпотентно по Idempotency-Key)",
        operation_id="createPayment",
        status_code=202,
        responses=conflict_responses,
    )
    router.add_api_route(
        "/payments/{payment_id}",
        controller.get_payment,
        methods=["GET"],
        tags=[PAYMENTS_TAG],
        summary="Получить платёж по id",
        operation_id="getPaymentById",
        responses=not_found_responses,
    )
    router.add_api_route(
        HEALTHCHECK_PATH,
        controller.healthcheck,
        methods=["GET"],
        include_in_schema=False,
    )
    return router
