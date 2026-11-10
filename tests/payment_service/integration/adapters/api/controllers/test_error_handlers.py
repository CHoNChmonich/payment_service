"""Общие свойства HTTP-адаптера: 503, 500, 404/405, ответы в схеме OpenAPI."""

import logging
from decimal import Decimal
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI

from payment_service.application import errors

from .test_payments import PAYMENT_ID

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
HANDLERS_LOGGER = "payment_service.adapters.api.error_handlers"
PAYMENT_URL = f"/api/v1/payments/{PAYMENT_ID}"


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestApplicationErrors:
    async def test__storage_unavailable(
        self, payment_service: Mock, client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Setup
        error = errors.StorageUnavailable()
        error.__cause__ = ConnectionRefusedError("отказ в соединении")
        payment_service.get.side_effect = error

        # Call
        with caplog.at_level(logging.WARNING, logger=HANDLERS_LOGGER):
            response = await client.get(PAYMENT_URL)

        # Assert
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {
            "error": "Хранилище временно недоступно, повторите попытку",
            "code": "storage_unavailable",
            "context": {},
        }
        # Ожидаемый временный сбой: одно предупреждение без трассировки, но с типом причины
        records = [record for record in caplog.records if record.name == HANDLERS_LOGGER]
        assert [record.levelno for record in records] == [logging.WARNING]
        assert records[0].exc_info is None
        assert records[0].getMessage().endswith("причина: ConnectionRefusedError")

    async def test__uncategorized_error_is_a_defect(
        self, payment_service: Mock, client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Setup
        class Uncategorized(errors.Error):
            message_template = "ошибка без категории"

        payment_service.get.side_effect = Uncategorized()

        # Call
        with caplog.at_level(logging.ERROR, logger=HANDLERS_LOGGER):
            response = await client.get(PAYMENT_URL)

        # Assert: 500 без текста, но с трассировкой в логе
        assert response.status_code == 500
        assert response.json() == {
            "error": "Внутренняя ошибка сервера",
            "code": "uncategorized",
            "context": {},
        }
        (record,) = [record for record in caplog.records if record.name == HANDLERS_LOGGER]
        assert record.exc_info is not None

    async def test__decimal_in_context_is_a_string(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Setup
        class AmountTooHigh(errors.RuleViolationError):
            message_template = "Слишком много: {amount}"

        payment_service.get.side_effect = AmountTooHigh(amount=Decimal("19.90"))

        # Call
        response = await client.get(PAYMENT_URL)

        # Assert
        assert response.status_code == 400
        assert response.json()["context"] == {"amount": "19.90"}


class TestUnexpectedError:
    async def test__unexpected_exception_without_details(
        self, app: FastAPI, payment_service: Mock, auth_headers: dict[str, str]
    ) -> None:
        # Setup
        payment_service.get.side_effect = RuntimeError("секрет не должен утечь")
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)

        # Call
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", headers=auth_headers
        ) as client:
            response = await client.get(PAYMENT_URL)

        # Assert
        assert response.status_code == 500
        assert response.json() == {
            "error": "Внутренняя ошибка сервера",
            "code": "internal_error",
            "context": {},
        }


class TestRouting:
    async def test__unknown_route(self, client: httpx.AsyncClient) -> None:
        # Call
        response = await client.get("/api/v1/nope")

        # Assert
        assert response.status_code == 404
        assert response.json() == {"error": "Not Found", "code": "http_error", "context": {}}

    async def test__method_not_allowed(self, client: httpx.AsyncClient) -> None:
        # Call
        response = await client.delete(PAYMENT_URL)

        # Assert
        assert response.status_code == 405
        assert response.json()["code"] == "http_error"
        assert response.headers["Allow"] == "GET"  # заголовки Starlette сохраняются


class TestOpenApiContract:
    def test__responses_without_422(self, app: FastAPI) -> None:
        # Call
        paths = app.openapi()["paths"]

        # Assert
        assert set(paths["/api/v1/payments"]["post"]["responses"]) == {
            "202",
            "400",
            "401",
            "409",
            "500",
            "503",
        }
        assert set(paths["/api/v1/payments/{payment_id}"]["get"]["responses"]) == {
            "200",
            "400",
            "401",
            "404",
            "500",
            "503",
        }
        assert "HTTPValidationError" not in app.openapi()["components"]["schemas"]
