import asyncio
import logging
import time
from collections.abc import Callable
from unittest.mock import Mock

import pytest

from payment_service.adapters import outbox_poller
from payment_service.application import errors, interfaces


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def relay() -> Mock:
    return Mock(interfaces.EventRelay)


@pytest.fixture(scope="function")
def stop() -> asyncio.Event:
    return asyncio.Event()


def _settings(idle_interval: float) -> outbox_poller.Settings:
    return outbox_poller.Settings.model_validate({"OUTBOX_POLLER_IDLE_INTERVAL": idle_interval})


def _results_then_stop(stop: asyncio.Event, *results: int | Exception) -> Callable[[], int]:
    """Побочный эффект вызовов доставки: результаты по очереди, на последнем — остановка."""
    pending = list(results)

    def effect() -> int:
        result = pending.pop(0)
        if not pending:
            stop.set()
        if isinstance(result, Exception):
            raise result
        return result

    return effect


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRun:
    async def test__no_pause_while_publishing(self, relay: Mock, stop: asyncio.Event) -> None:
        # Setup: пауза велика — если бы цикл её делал, тест упёрся бы в таймаут
        relay.relay.side_effect = _results_then_stop(stop, 5, 3, 0)
        heartbeat = Mock()
        poller = outbox_poller.OutboxPoller(relay, _settings(30), heartbeat=heartbeat)

        # Call
        async with asyncio.timeout(1):
            await poller.run(stop)

        # Assert
        assert relay.relay.await_count == 3
        assert heartbeat.call_count == 3

    async def test__pause_when_idle(self, relay: Mock, stop: asyncio.Event) -> None:
        # Setup
        calls: list[float] = []

        def idle() -> int:
            calls.append(time.monotonic())
            if len(calls) == 2:
                stop.set()
            return 0

        relay.relay.side_effect = idle
        poller = outbox_poller.OutboxPoller(relay, _settings(0.2))

        # Call
        async with asyncio.timeout(5):
            await poller.run(stop)

        # Assert: после пустого вызова — пауза перед следующим
        assert len(calls) == 2
        assert calls[1] - calls[0] >= 0.15

    async def test__stop_interrupts_pause(self, relay: Mock, stop: asyncio.Event) -> None:
        # Setup
        relay.relay.return_value = 0
        poller = outbox_poller.OutboxPoller(relay, _settings(30))

        # Call
        async with asyncio.timeout(1):
            task = asyncio.create_task(poller.run(stop))
            await asyncio.sleep(0.05)  # цикл в паузе
            stop.set()
            await task

        # Assert
        assert relay.relay.await_count == 1

    async def test__unavailable_is_warning_and_pause(
        self, relay: Mock, stop: asyncio.Event, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Setup
        calls: list[float] = []
        results = _results_then_stop(stop, errors.BrokerUnavailable(), 0)

        def recorded() -> int:
            calls.append(time.monotonic())
            return results()

        relay.relay.side_effect = recorded
        poller = outbox_poller.OutboxPoller(relay, _settings(0.2))

        # Call
        with caplog.at_level(logging.WARNING, logger="payment_service.adapters.outbox_poller"):
            async with asyncio.timeout(5):
                await poller.run(stop)

        # Assert: после сбоя — пауза, а не горячий цикл по недоступному брокеру
        assert len(calls) == 2
        assert calls[1] - calls[0] >= 0.15
        assert [record.levelno for record in caplog.records] == [logging.WARNING]

    async def test__other_error_stops_loop(self, relay: Mock, stop: asyncio.Event) -> None:
        # Setup
        relay.relay.side_effect = RuntimeError("дефект")
        poller = outbox_poller.OutboxPoller(relay, _settings(0.01))

        # Call and Assert: наружу — процесс упадёт заметно
        with pytest.raises(RuntimeError, match="дефект"):
            await poller.run(stop)
