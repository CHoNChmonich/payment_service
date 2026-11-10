"""Тесты контроллеров: настоящее приложение FastAPI поверх моков порта сценариев и аутентификатора.

Клиент по умолчанию шлёт ключ, а мок аутентификатора пропускает любой: тесты сценариев о
проверке ключа не думают. Что каждый маршрут её действительно требует, проверяет
``test_auth.py`` по реестру всех операций.
"""

from collections.abc import AsyncIterator
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI

from payment_service.adapters.api import V1Controller, create_app
from payment_service.application import interfaces


@pytest.fixture(scope="function")
def api_key() -> str:
    return "test-api-key-0123456789"


@pytest.fixture(scope="function")
def auth_headers(api_key: str) -> dict[str, str]:
    return {"X-API-Key": api_key}


@pytest.fixture(scope="function")
def authenticator() -> Mock:
    return Mock(interfaces.Authenticator)


@pytest.fixture(scope="function")
def payment_service() -> Mock:
    return Mock(interfaces.Payment)


@pytest.fixture(scope="function")
def controller(authenticator: Mock, payment_service: Mock) -> V1Controller:
    return V1Controller(authenticator=authenticator, payment=payment_service)


@pytest.fixture(scope="function")
def app(controller: V1Controller) -> FastAPI:
    return create_app(v1_controller=controller)


@pytest.fixture(scope="function")
async def client(app: FastAPI, auth_headers: dict[str, str]) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", headers=auth_headers
    ) as client:
        yield client
