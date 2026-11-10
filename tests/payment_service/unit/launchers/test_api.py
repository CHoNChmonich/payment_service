import importlib
from types import ModuleType

import pytest
from fastapi import FastAPI

from payment_service.adapters import database, payment_gateway, webhook


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def launcher(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Composition root читает настройки при импорте; БД при этом не открывается
    monkeypatch.setenv("POSTGRES_USER", "payments")
    monkeypatch.setenv("POSTGRES_PASSWORD", "payments")
    monkeypatch.setenv("POSTGRES_DB", "payments")
    monkeypatch.setenv("AUTH_API_KEY", "k" * 32)
    return importlib.import_module("payment_service.launchers.api")


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestComposition:
    async def test__builds_without_connecting(self, launcher: ModuleType) -> None:
        # Call: вход и выход UoW без запросов — соединение с БД не открывается
        async with launcher.Adapters.uow_factory() as uow:
            # Assert
            assert type(uow) is database.UnitOfWork
        assert isinstance(launcher.app, FastAPI)

    def test__payment_ports_under_retries(self, launcher: ModuleType) -> None:
        # Assert: сервис собран так же, как у консьюмера — семантика не зависит от процесса
        payment = launcher.Application.payment
        assert isinstance(payment.gateway, payment_gateway.RetryingPaymentGateway)
        assert isinstance(payment.notifier, webhook.RetryingWebhookNotifier)
