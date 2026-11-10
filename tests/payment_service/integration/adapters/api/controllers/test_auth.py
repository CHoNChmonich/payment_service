"""Аутентификация HTTP-адаптера: каждая операция требует ключ, разбор заголовка, 401."""

import re
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import Mock, call

import httpx
import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute

from payment_service.adapters.api import V1Controller
from payment_service.adapters.api.v1 import create_router
from payment_service.application import errors
from payment_service.application.dtos import MAX_API_KEY_LENGTH

from .test_payments import IDEMPOTENCY_HEADERS, PAYMENT_ID, VALID_BODY, _payment_info

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
# Реестр: метод, шаблон пути и валидное тело каждой операции. С невалидным запросом FastAPI
# ответил бы 400 раньше обработчика, и проверка аутентификации ничего бы не доказала. Реестр
# сверяется с маршрутами роутера: новая операция без записи роняет test__registry_matches_routes.
VALID_REQUESTS: dict[str, tuple[str, str, dict[str, Any] | None]] = {
    "createPayment": ("POST", "/payments", VALID_BODY),
    "getPaymentById": ("GET", "/payments/{payment_id}", None),
}
# Маршруты без аутентификации: вне схемы OpenAPI и перечислены явно.
PUBLIC_PATHS = {"/healthcheck"}
PREFIX = "/api/v1"
WWW_AUTHENTICATE = 'ApiKey header="X-API-Key"'


@pytest.fixture(scope="function")
async def anonymous_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


async def _send(
    client: httpx.AsyncClient, operation_id: str, headers: dict[str, str] | None = None
) -> httpx.Response:
    method, path, body = VALID_REQUESTS[operation_id]
    url = PREFIX + re.sub(r"\{\w+\}", str(PAYMENT_ID), path)
    return await client.request(
        method, url, json=body, headers={**IDEMPOTENCY_HEADERS, **(headers or {})}
    )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestEveryOperation:
    def test__registry_matches_routes(self, controller: V1Controller) -> None:
        # Call: все маршруты роутера, включая скрытые из схемы
        routes = create_router(controller, prefix="").routes

        # Assert: защищённые — ровно реестр с теми же методом и путём; публичные — по списку
        # и вне схемы
        assert all(isinstance(route, APIRoute) for route in routes)
        api_routes = [route for route in routes if isinstance(route, APIRoute)]
        protected = sorted(
            (route.operation_id or "", sorted(route.methods or ()), route.path)
            for route in api_routes
            if route.path not in PUBLIC_PATHS
        )
        registry = sorted((op, [method], path) for op, (method, path, _) in VALID_REQUESTS.items())
        assert protected == registry
        public = [route for route in api_routes if route.path in PUBLIC_PATHS]
        assert {route.path for route in public} == PUBLIC_PATHS
        assert not any(route.include_in_schema for route in public)

    @pytest.mark.parametrize("operation_id", sorted(VALID_REQUESTS))
    async def test__credentials_missing(
        self,
        operation_id: str,
        anonymous_client: httpx.AsyncClient,
        authenticator: Mock,
        payment_service: Mock,
    ) -> None:
        # Call
        response = await _send(anonymous_client, operation_id)

        # Assert
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == WWW_AUTHENTICATE
        assert response.json() == {
            "error": "Требуется API-ключ в заголовке X-API-Key",
            "code": "credentials_missing",
            "context": {},
        }
        assert authenticator.method_calls == []
        assert payment_service.method_calls == []

    @pytest.mark.parametrize("operation_id", sorted(VALID_REQUESTS))
    async def test__credentials_invalid(
        self,
        operation_id: str,
        client: httpx.AsyncClient,
        api_key: str,
        authenticator: Mock,
        payment_service: Mock,
    ) -> None:
        # Setup
        authenticator.authenticate.side_effect = errors.CredentialsInvalid()

        # Call
        response = await _send(client, operation_id)

        # Assert: ключ ушёл в порт, сценарий не вызван
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == WWW_AUTHENTICATE
        assert response.json() == {
            "error": "API-ключ недействителен",
            "code": "credentials_invalid",
            "context": {},
        }
        assert authenticator.method_calls == [call.authenticate(api_key)]
        assert payment_service.method_calls == []


class TestApiKeyHeader:
    @pytest.mark.parametrize(
        "value",
        [
            b"",
            b" ",
            b"\t",
            b"key with space",
            b"key\twith\ttab",
            b"key\xa0",
            b"key\r",
            b"\xc3\xa9key",
            b"a" * (MAX_API_KEY_LENGTH + 1),
        ],
    )
    async def test__credentials_missing(
        self,
        value: bytes,
        anonymous_client: httpx.AsyncClient,
        authenticator: Mock,
        payment_service: Mock,
    ) -> None:
        # Call
        response = await anonymous_client.get(
            f"/api/v1/payments/{PAYMENT_ID}", headers={b"X-API-Key": value}
        )

        # Assert: мусор до сверки не доходит
        assert response.status_code == 401
        assert response.json()["code"] == "credentials_missing"
        assert authenticator.method_calls == []
        assert payment_service.method_calls == []

    @pytest.mark.parametrize(
        ("value", "api_key"),
        [
            ("abc", "abc"),
            ("\tabc ", "abc"),
            ("a1.b-c_d~e+f/g==", "a1.b-c_d~e+f/g=="),
            ("!#$%&'()*,;<>?@[]^`{|}", "!#$%&'()*,;<>?@[]^`{|}"),
            ("a" * MAX_API_KEY_LENGTH, "a" * MAX_API_KEY_LENGTH),
        ],
    )
    async def test__key_passed_to_port(
        self,
        value: str,
        api_key: str,
        anonymous_client: httpx.AsyncClient,
        authenticator: Mock,
        payment_service: Mock,
    ) -> None:
        # Setup
        payment_service.get.return_value = _payment_info()

        # Call
        response = await anonymous_client.get(
            f"/api/v1/payments/{PAYMENT_ID}", headers={"X-API-Key": value}
        )

        # Assert
        assert response.status_code == 200
        assert authenticator.method_calls == [call.authenticate(api_key)]
        assert payment_service.method_calls == [call.get(PAYMENT_ID)]

    async def test__header_name_case_insensitive(
        self, anonymous_client: httpx.AsyncClient, authenticator: Mock, payment_service: Mock
    ) -> None:
        # Setup
        payment_service.get.return_value = _payment_info()

        # Call
        response = await anonymous_client.get(
            f"/api/v1/payments/{PAYMENT_ID}", headers={"x-api-key": "abc"}
        )

        # Assert
        assert response.status_code == 200
        assert authenticator.method_calls == [call.authenticate("abc")]

    async def test__first_header_wins(
        self, anonymous_client: httpx.AsyncClient, authenticator: Mock, payment_service: Mock
    ) -> None:
        # Setup
        payment_service.get.return_value = _payment_info()

        # Call
        response = await anonymous_client.get(
            f"/api/v1/payments/{PAYMENT_ID}",
            headers=[("X-API-Key", "first"), ("X-API-Key", "second")],
        )

        # Assert
        assert response.status_code == 200
        assert authenticator.method_calls == [call.authenticate("first")]


class TestHealthcheck:
    async def test__healthcheck_without_key(
        self, anonymous_client: httpx.AsyncClient, authenticator: Mock
    ) -> None:
        # Call
        response = await anonymous_client.get("/api/v1/healthcheck")

        # Assert: что ручки нет в схеме, проверяет test__registry_matches_routes
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        assert authenticator.method_calls == []


class TestOpenApi:
    def test__api_key_security(self, app: FastAPI) -> None:
        # Call
        schema = app.openapi()

        # Assert: требование на корне схемы, заголовок ключа скрыт из параметров, 401 у каждой
        # операции; Idempotency-Key в параметрах остаётся
        assert schema["components"]["securitySchemes"] == {
            "apiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
        }
        assert schema["security"] == [{"apiKeyAuth": []}]
        operations = [op for methods in schema["paths"].values() for op in methods.values()]
        assert {op["operationId"] for op in operations} == set(VALID_REQUESTS)
        for operation in operations:
            assert "401" in operation["responses"]
            assert "security" not in operation
            header_params = [
                param["name"]
                for param in operation.get("parameters", [])
                if param["in"] == "header"
            ]
            assert header_params in ([], ["Idempotency-Key"])
