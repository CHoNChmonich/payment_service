"""Цикл консьюмера библиотеки ``rabbitmq``: обработчик и политика — подделки, брокер — фейк.

Разбор тела и таблица политики проверяются у входящих адаптеров (``new_payments``,
``processed_payments``); здесь — механика: подписка, ack, DLQ, повторы, бюджет, остановка.
"""

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from faststream import AckPolicy
from faststream.rabbit import RabbitMessage

from payment_service.adapters import rabbitmq

from .support import FakeBroker, FakeRaw, config, message

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
BODY = b'{"event_id": "e-1"}'


class Rejected(Exception):
    """Отказ, после которого сообщение — в DLQ."""


class Outage(Exception):
    """Временный сбой — повтор на месте."""


def _policy(error: BaseException) -> rabbitmq.Action:
    match error:
        case Rejected():
            return rabbitmq.Action.DEAD_LETTER
        case Outage() | TimeoutError():
            return rabbitmq.Action.RETRY
        case _:
            return rabbitmq.Action.STOP


@pytest.fixture(scope="function")
def handler() -> AsyncMock:
    return AsyncMock(return_value=True)


@pytest.fixture(scope="function")
def log() -> list[str]:
    return []


@pytest.fixture(scope="function")
def dead_letters(log: list[str]) -> Mock:
    letters = Mock(rabbitmq.DeadLetters)
    letters.publish.side_effect = lambda message, error, attempts: log.append(
        f"dlq {message.message_id} {type(error).__name__} {attempts}"
    )
    return letters


@pytest.fixture(scope="function")
def stop() -> asyncio.Event:
    return asyncio.Event()


async def _run(
    broker: FakeBroker,
    handler: AsyncMock,
    dead_letters: Mock,
    stop: asyncio.Event,
    **overrides: float,
) -> list[str]:
    heartbeats: list[str] = []
    consumer = rabbitmq.QueueConsumer(
        handler=handler,
        action_for=_policy,
        dead_letters=dead_letters,
        config=config(**overrides),
        broker=cast(Any, broker),
        heartbeat=lambda: heartbeats.append("beat"),
    )
    async with asyncio.timeout(5):
        await consumer.run(stop)
    if broker.handler_error is not None:
        raise broker.handler_error
    return heartbeats


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRun:
    @pytest.mark.parametrize("applied", [True, False], ids=["applied", "duplicate"])
    async def test__declares_subscribes_handles_acks(
        self,
        handler: AsyncMock,
        dead_letters: Mock,
        stop: asyncio.Event,
        log: list[str],
        applied: bool,
    ) -> None:
        # Setup
        handler.return_value = applied
        broker = FakeBroker([message(BODY, log, message_id="m-5")], stop, log)

        # Call
        heartbeats = await _run(broker, handler, dead_letters, stop)

        # Assert: обработчик получил тело; повтор (False) подтверждается так же
        handler.assert_awaited_once_with(BODY)
        assert broker.subscription == {
            "queue": "payments.new",
            "exchange": "payments",
            "declare": (False, False),  # объявляет только declare(), подписчик — нет
            "ack_policy": AckPolicy.MANUAL,
        }
        # Топология объявлена до старта подписчика (порядок объявлений — test_topology.py)
        assert log.index("connect") < log.index("declare_queue payments.new") < log.index("start")
        assert log[-2:] == ["ack m-5", "stop"]
        assert heartbeats

    async def test__broker_not_answering_skips_heartbeat(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        broker = FakeBroker([], stop, log, ping_ok=False)

        # Call
        heartbeats = await _run(broker, handler, dead_letters, stop)

        # Assert
        assert heartbeats == []

    async def test__stop_signal_ends_loop_and_stops_broker(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: сообщений нет, остановка приходит извне
        broker = FakeBroker([], stop, log)

        async def signal() -> None:
            await asyncio.sleep(0.05)
            stop.set()

        # Call
        async with asyncio.TaskGroup() as group:
            group.create_task(signal())
            heartbeats = await _run(broker, handler, dead_letters, stop)

        # Assert: успешный ping ставит метку «жив» и без сообщений
        assert log[-1] == "stop"
        assert heartbeats


class TestDeadLetter:
    async def test__rejection_goes_to_dlq_and_ack(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        handler.side_effect = Rejected()

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), handler, dead_letters, stop)

        # Assert: одна попытка, DLQ, затем ack
        assert handler.await_count == 1
        assert log[-3:] == ["dlq m-1 Rejected 1", "ack m-1", "stop"]

    async def test__dlq_outage_is_retried_then_acked(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: первая запись в DLQ не подтверждена, вторая прошла
        handler.side_effect = Rejected()
        dead_letters.publish.side_effect = [
            rabbitmq.DeadLetterUnavailable("нет связи"),
            None,
        ]

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), handler, dead_letters, stop)

        # Assert: обработчик не повторялся — повторялась только запись в DLQ
        assert handler.await_count == 1
        assert dead_letters.publish.await_count == 2
        assert log[-2:] == ["ack m-1", "stop"]

    async def test__permanent_dlq_failure_stops_without_ack(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        handler.side_effect = Rejected()
        dead_letters.publish.side_effect = rabbitmq.DeadLetterFailed("нет прав")

        # Call and Assert
        with pytest.raises(rabbitmq.DeadLetterFailed):
            await _run(FakeBroker([message(BODY, log)], stop, log), handler, dead_letters, stop)
        assert "ack m-1" not in log
        assert log[-1] == "stop"


class TestRetry:
    async def test__outage_is_retried_in_place(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: два сбоя, третья попытка успешна
        handler.side_effect = [Outage(), Outage(), True]

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), handler, dead_letters, stop)

        # Assert: без DLQ, один ack
        assert handler.await_count == 3
        dead_letters.publish.assert_not_called()
        assert log.count("ack m-1") == 1

    async def test__attempt_deadline_is_retried(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: первая попытка зависает дольше дедлайна 0.05 с, вторая успешна
        calls = 0

        async def handle(body: bytes) -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(1)
            return True

        handler.side_effect = handle

        # Call
        await _run(
            FakeBroker([message(BODY, log)], stop, log),
            handler,
            dead_letters,
            stop,
            handler_timeout=0.05,
        )

        # Assert
        assert calls == 2
        assert "ack m-1" in log

    async def test__exhausted_budget_stops_without_ack(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        handler.side_effect = Outage()

        # Call and Assert
        with pytest.raises(rabbitmq.RetryBudgetExhausted) as excinfo:
            await _run(
                FakeBroker([message(BODY, log)], stop, log),
                handler,
                dead_letters,
                stop,
                retry_budget=0.05,
            )
        assert isinstance(excinfo.value.__cause__, Outage)
        assert handler.await_count >= 2
        assert "ack m-1" not in log
        dead_letters.publish.assert_not_called()

    async def test__stop_signal_interrupts_pause_without_ack(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: сбой, во время паузы приходит остановка
        async def handle(body: bytes) -> bool:
            stop.set()
            raise Outage()

        handler.side_effect = handle

        # Call
        await _run(
            FakeBroker([message(BODY, log)], stop, log),
            handler,
            dead_letters,
            stop,
            retry_backoff=5,
            retry_backoff_max=5,
            retry_budget=60,
        )

        # Assert: одна попытка, без ack — сообщение вернётся в очередь при закрытии канала
        assert handler.await_count == 1
        assert "ack m-1" not in log


class TestStop:
    @pytest.mark.parametrize("error", [RuntimeError("дефект"), KeyError("k")])
    async def test__defect_stops_process_without_ack(
        self,
        handler: AsyncMock,
        dead_letters: Mock,
        stop: asyncio.Event,
        log: list[str],
        error: Exception,
    ) -> None:
        # Setup
        handler.side_effect = error

        # Call and Assert
        with pytest.raises(type(error)):
            await _run(FakeBroker([message(BODY, log)], stop, log), handler, dead_letters, stop)
        assert "ack m-1" not in log
        dead_letters.publish.assert_not_called()
        assert log[-1] == "stop"


class TestAck:
    async def test__ack_failure_is_logged_not_raised(
        self, handler: AsyncMock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        item: RabbitMessage = message(BODY, log)
        cast(FakeRaw, item.raw_message).ack_error = RuntimeError("канал закрыт")

        # Call: не поднимает
        await _run(FakeBroker([item], stop, log), handler, dead_letters, stop)

        # Assert
        assert "ack m-1" not in log
        assert log[-1] == "stop"
