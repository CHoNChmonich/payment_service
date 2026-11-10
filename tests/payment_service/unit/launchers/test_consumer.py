import importlib
from types import ModuleType

import pytest

from payment_service.adapters import database, new_payments, payment_gateway, webhook


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def launcher(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Composition root читает настройки при импорте; ни БД, ни RabbitMQ при этом не открываются
    monkeypatch.setenv("POSTGRES_USER", "payments")
    monkeypatch.setenv("POSTGRES_PASSWORD", "payments")
    monkeypatch.setenv("POSTGRES_DB", "payments")
    monkeypatch.setenv("POSTGRES_LOCK_TIMEOUT_MS", "20000")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    return importlib.import_module("payment_service.launchers.consumer")


def _db(lock_timeout_ms: int | None) -> database.Settings:
    return database.Settings.model_validate(
        {
            "POSTGRES_USER": "u",
            "POSTGRES_PASSWORD": "p",
            "POSTGRES_DB": "d",
            "POSTGRES_LOCK_TIMEOUT_MS": lock_timeout_ms,
        }
    )


def _payments(handler_timeout: float = 90) -> new_payments.Settings:
    return new_payments.Settings.model_validate(
        {"RABBITMQ_URL": "amqp://x", "NEW_PAYMENTS_HANDLER_TIMEOUT": handler_timeout}
    )


GATEWAY = payment_gateway.Settings.model_validate({})
WEBHOOK = webhook.Settings.model_validate({})


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCheckTimings:
    def test__defaults_are_consistent(self, launcher: ModuleType) -> None:
        # Call: не поднимает — 20 с ≥ 18 с удержания, 90 с ≥ 40 + 18 + 18
        launcher.check_timings(_db(20_000), _payments(), GATEWAY, WEBHOOK)

    def test__lock_timeout_is_required(self, launcher: ModuleType) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="обязателен"):
            launcher.check_timings(_db(None), _payments(), GATEWAY, WEBHOOK)

    def test__lock_timeout_below_longest_hold(self, launcher: ModuleType) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="худшего удержания"):
            launcher.check_timings(_db(10_000), _payments(), GATEWAY, WEBHOOK)

    def test__handler_timeout_below_needed(self, launcher: ModuleType) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="NEW_PAYMENTS_HANDLER_TIMEOUT"):
            launcher.check_timings(_db(20_000), _payments(60), GATEWAY, WEBHOOK)


class TestComposition:
    async def test__builds_without_connecting(self, launcher: ModuleType) -> None:
        # Call: вход и выход UoW без запросов — соединение с БД не открывается
        async with launcher.Adapters.uow_factory() as uow:
            # Assert
            assert type(uow) is database.UnitOfWork
        # Консьюмер и DLQ — на одном брокере: подключает его только запуск консьюмера
        consumer = launcher.Consumers.new_payments
        assert isinstance(consumer, new_payments.NewPaymentsConsumer)
        assert consumer._broker is launcher.Adapters.dead_letters._broker

    def test__payment_ports_under_retries(self, launcher: ModuleType) -> None:
        # Assert
        payment = launcher.Application.payment
        assert isinstance(payment.gateway, payment_gateway.RetryingPaymentGateway)
        assert isinstance(payment.notifier, webhook.RetryingWebhookNotifier)
