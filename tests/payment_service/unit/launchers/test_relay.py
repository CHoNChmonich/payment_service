import importlib
from types import ModuleType

import pytest

from payment_service.adapters import database, outbox_poller, payment_events


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def launcher(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Composition root читает настройки при импорте; ни БД, ни RabbitMQ при этом не открываются
    monkeypatch.setenv("POSTGRES_USER", "payments")
    monkeypatch.setenv("POSTGRES_PASSWORD", "payments")
    monkeypatch.setenv("POSTGRES_DB", "payments")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    monkeypatch.setenv("RELAY_BATCH_SIZE", "7")
    return importlib.import_module("payment_service.launchers.relay")


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestComposition:
    async def test__builds_without_connecting(self, launcher: ModuleType) -> None:
        # Call: вход и выход UoW без запросов — соединение с БД не открывается
        async with launcher.Adapters.uow_factory() as uow:
            # Assert
            assert type(uow) is database.UnitOfWork
            assert isinstance(uow.outbox, database.repositories.OutboxRepo)
        assert isinstance(launcher.Pollers.outbox, outbox_poller.OutboxPoller)

    async def test__publisher_not_opened(self, launcher: ModuleType) -> None:
        # Assert: брокер подключается хуком запуска, а не при импорте
        assert isinstance(launcher.Adapters.publisher, payment_events.EventPublisher)
        with pytest.raises(RuntimeError, match="aopen"):
            await launcher.Adapters.publisher.publish([])

    def test__batch_size_from_settings(self, launcher: ModuleType) -> None:
        # Assert
        assert launcher.Application.relay.batch_size == 7
