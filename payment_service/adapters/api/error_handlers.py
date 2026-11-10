"""Перевод ошибок ядра, ошибок разбора запроса и ошибок маршрутизации в HTTP-ответы единой формы.

Отображаются **категории** ошибок (``NotFoundError`` → 404, ``ConflictError`` → 409,
``RuleViolationError`` → 400, ``AuthenticationError`` → 401, ``UnavailableError`` → 503), а не
каждый класс: новая ошибка ядра не требует правок здесь. Тело всегда одно:
``{"error", "code", "context"}``.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from payment_service.application import errors

logger = logging.getLogger(__name__)

INTERNAL_ERROR_MESSAGE = "Внутренняя ошибка сервера"
# Значение WWW-Authenticate для 401: схема нестандартная, но RFC 9110 требует заголовок.
WWW_AUTHENTICATE = 'ApiKey header="X-API-Key"'


@dataclass(frozen=True)
class ApiError:
    """Ошибка в терминах HTTP перед сериализацией в ответ.

    Attributes:
        status_code: HTTP-статус ответа.
        message: Текст ошибки для человека.
        code: Машинный код ошибки в snake_case.
        context: Детали ошибки для клиента; сериализуются в JSON.
        headers: Дополнительные заголовки ответа (например, ``Retry-After``).
    """

    status_code: int
    message: str
    code: str
    context: Mapping[str, object] = field(default_factory=dict)
    headers: Mapping[str, str] | None = None


def register_error_handlers(app: FastAPI) -> None:
    """Регистрирует обработчики ошибок приложения.

    Регистрация идёт декоратором ``app.exception_handler``, а не ``add_exception_handler``:
    так сигнатуры обработчиков проходят ``mypy --strict`` без ``type: ignore``.

    Args:
        app: Приложение, которому добавляются обработчики.
    """

    @app.exception_handler(errors.Error)
    async def handle_app_error(_request: Request, exc: errors.Error) -> JSONResponse:
        """Переводит ошибку ядра в ответ по её категории.

        Временная недоступность хранилища (503) — ожидаемая ситуация и пишется в лог
        предупреждением без трассировки, но с типом исходной ошибки (``DBAPIError``,
        ``ConnectionRefusedError`` …): без него отказ в соединении и ошибка сервера БД
        неразличимы. Текст причины не пишется — в нём бывают SQL и параметры. Остальные
        ответы 5xx логируются с трассировкой.

        Args:
            _request: Запрос; не используется.
            exc: Ошибка ядра.

        Returns:
            JSON-ответ единой формы.
        """
        api_error = _to_api_error(exc)
        if api_error.status_code == 503:
            cause = type(exc.__cause__).__name__ if exc.__cause__ is not None else "-"
            logger.warning("%s: %s; причина: %s", type(exc).__name__, exc.message, cause)
        elif api_error.status_code >= 500:
            logger.exception("%s: %s", type(exc).__name__, exc.message)
        return _response(api_error)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Переводит ошибку разбора запроса в 400 с перечнем нарушений.

        Args:
            _request: Запрос; не используется.
            exc: Ошибка валидации FastAPI.

        Returns:
            JSON-ответ единой формы с ``code: validation_error``.
        """
        return _response(
            ApiError(
                status_code=400,
                message="Некорректный запрос",
                code="validation_error",
                context={"details": _validation_details(exc)},
            )
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        """Переводит ошибки маршрутизации (404 неизвестный путь, 405 метод) в единую форму тела.

        Args:
            _request: Запрос; не используется.
            exc: HTTP-исключение Starlette.

        Returns:
            JSON-ответ единой формы со статусом и заголовками исходного исключения.
        """
        return _response(
            ApiError(
                status_code=exc.status_code,
                message=str(exc.detail),
                code="http_error",
                headers=exc.headers,
            )
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(_request: Request, _exc: Exception) -> JSONResponse:
        """Отвечает 500 без подробностей на любое необработанное исключение.

        Трассировку пишет сервер: ``ServerErrorMiddleware`` перебрасывает исключение после
        ответа, логировать здесь — дублировать запись.

        Args:
            _request: Запрос; не используется.
            _exc: Исключение; не используется.

        Returns:
            JSON-ответ единой формы с ``code: internal_error``.
        """
        return _response(
            ApiError(status_code=500, message=INTERNAL_ERROR_MESSAGE, code="internal_error")
        )


def _to_api_error(exc: errors.Error) -> ApiError:
    """Отображает категорию ошибки ядра на HTTP-статус.

    Ошибка ядра без категории — дефект: она отдаётся как 500, а не маскируется под 400.

    Args:
        exc: Ошибка ядра.

    Returns:
        Ошибка в терминах HTTP с текстом, кодом и контекстом исходной.
    """
    if isinstance(exc, errors.NotFoundError):
        return ApiError(404, exc.message, exc.code, exc.context)
    if isinstance(exc, errors.ConflictError):
        return ApiError(409, exc.message, exc.code, exc.context)
    if isinstance(exc, errors.RuleViolationError):
        return ApiError(400, exc.message, exc.code, exc.context)
    if isinstance(exc, errors.AuthenticationError):
        return ApiError(
            401, exc.message, exc.code, exc.context, headers={"WWW-Authenticate": WWW_AUTHENTICATE}
        )
    if isinstance(exc, errors.UnavailableError):
        return ApiError(503, exc.message, exc.code, exc.context, headers={"Retry-After": "1"})
    return ApiError(500, INTERNAL_ERROR_MESSAGE, exc.code)


def _validation_details(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Сводит ошибки валидации к сериализуемому перечню.

    Берутся только ``loc``, ``msg`` и ``type``: в ``ctx`` pydantic кладёт объекты исключений,
    которые в JSON не сериализуются.

    Args:
        exc: Ошибка валидации FastAPI.

    Returns:
        Список нарушений с путём до поля, сообщением и типом.
    """
    return [
        {
            "loc": list(error.get("loc", ())),
            "msg": error.get("msg", ""),
            "type": error.get("type", ""),
        }
        for error in exc.errors()
    ]


def _response(error: ApiError) -> JSONResponse:
    """Сериализует ошибку в JSON-ответ единой формы.

    ``Decimal`` в контексте уходит строкой, как и деньги в ответах; UUID и перечисления —
    значением.

    Args:
        error: Ошибка в терминах HTTP.

    Returns:
        JSON-ответ с телом ``{"error", "code", "context"}``, статусом и заголовками ошибки.
    """
    content = {"error": error.message, "code": error.code, "context": error.context}
    return JSONResponse(
        status_code=error.status_code,
        content=jsonable_encoder(content, custom_encoder={Decimal: str}),
        headers=dict(error.headers) if error.headers else None,
    )
