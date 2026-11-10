import asyncio
import json
from typing import Any, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from payment_service.adapters import new_payments
from payment_service.application import dtos, errors, interfaces

from .support import FakeBroker, FakeRaw, message, settings

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")
BODY = json.dumps(
    {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "type": "payment_created",
        "payment_id": str(PAYMENT_ID),
    }
).encode()


@pytest.fixture(scope="function")
def payment() -> Mock:
    return Mock(interfaces.Payment)


@pytest.fixture(scope="function")
def log() -> list[str]:
    return []


@pytest.fixture(scope="function")
def dead_letters(log: list[str]) -> Mock:
    letters = Mock(new_payments.DeadLetters)
    letters.publish.side_effect = lambda message, error, attempts: log.append(
        f"dlq {message.message_id} {type(error).__name__} {attempts}"
    )
    return letters


@pytest.fixture(scope="function")
def stop() -> asyncio.Event:
    return asyncio.Event()


async def _run(
    broker: FakeBroker, payment: Mock, dead_letters: Mock, stop: asyncio.Event, **overrides: float
) -> list[str]:
    heartbeats: list[str] = []
    consumer = new_payments.NewPaymentsConsumer(
        handler=new_payments.NewPaymentsHandler(payment),
        dead_letters=dead_letters,
        settings=settings(**overrides),
        broker=cast(Any, broker),
        heartbeat=lambda: heartbeats.append("beat"),
    )
    async with asyncio.timeout(5):
        await consumer.run(stop)
    if broker.handler_error is not None:
        raise broker.handler_error
    return heartbeats


def _validation_error() -> ValidationError:
    try:
        dtos.PaymentInfo.model_validate({})
    except ValidationError as exc:
        return exc
    raise AssertionError("ожидалась ошибка валидации")


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRun:
    async def test__declares_subscribes_processes_acks(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        payment.notify.return_value = True
        broker = FakeBroker([message(BODY, log, message_id="m-5")], stop, log)

        # Call
        heartbeats = await _run(broker, payment, dead_letters, stop)

        # Assert
        payment.process.assert_awaited_once_with(PAYMENT_ID)
        payment.notify.assert_awaited_once_with(PAYMENT_ID)
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
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        broker = FakeBroker([], stop, log, ping_ok=False)

        # Call
        heartbeats = await _run(broker, payment, dead_letters, stop)

        # Assert
        assert heartbeats == []

    async def test__stop_signal_ends_loop_and_stops_broker(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: сообщений нет, остановка приходит извне
        broker = FakeBroker([], stop, log)

        async def signal() -> None:
            await asyncio.sleep(0.05)
            stop.set()

        # Call
        async with asyncio.TaskGroup() as group:
            group.create_task(signal())
            heartbeats = await _run(broker, payment, dead_letters, stop)

        # Assert: успешный ping ставит метку «жив» и без сообщений
        assert log[-1] == "stop"
        assert heartbeats


class TestDeadLetter:
    @pytest.mark.parametrize(
        ("error", "name"),
        [
            (errors.PaymentNotFound(id=PAYMENT_ID), "PaymentNotFound"),
            (errors.WebhookRejected(url="u", status_code=404), "WebhookRejected"),
            (errors.WebhookUnavailable(url="u"), "WebhookUnavailable"),
        ],
    )
    async def test__core_rejections_go_to_dlq_and_ack(
        self,
        payment: Mock,
        dead_letters: Mock,
        stop: asyncio.Event,
        log: list[str],
        error: Exception,
        name: str,
    ) -> None:
        # Setup
        payment.process.side_effect = error

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), payment, dead_letters, stop)

        # Assert: одна попытка, DLQ, затем ack
        assert log[-3:] == [f"dlq m-1 {name} 1", "ack m-1", "stop"]

    async def test__invalid_body_goes_to_dlq_without_processing(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Call
        await _run(FakeBroker([message(b"garbage", log)], stop, log), payment, dead_letters, stop)

        # Assert
        payment.process.assert_not_called()
        assert log[-3:] == ["dlq m-1 InvalidMessage 1", "ack m-1", "stop"]

    async def test__dlq_outage_is_retried_then_acked(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: первая запись в DLQ не подтверждена, вторая прошла
        payment.process.side_effect = errors.PaymentNotFound(id=PAYMENT_ID)
        dead_letters.publish.side_effect = [
            new_payments.DeadLetterUnavailable("нет связи"),
            None,
        ]

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), payment, dead_letters, stop)

        # Assert
        assert dead_letters.publish.await_count == 2
        assert log[-2:] == ["ack m-1", "stop"]

    async def test__permanent_dlq_failure_stops_without_ack(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        payment.process.side_effect = errors.PaymentNotFound(id=PAYMENT_ID)
        dead_letters.publish.side_effect = new_payments.DeadLetterFailed("нет прав")

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterFailed):
            await _run(FakeBroker([message(BODY, log)], stop, log), payment, dead_letters, stop)
        assert "ack m-1" not in log
        assert log[-1] == "stop"


class TestRetry:
    async def test__infrastructure_outage_is_retried_in_place(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: два сбоя хранилища, третья попытка успешна
        payment.process.side_effect = [
            errors.StorageUnavailable(),
            errors.StorageUnavailable(),
            None,
        ]
        payment.notify.return_value = True

        # Call
        await _run(FakeBroker([message(BODY, log)], stop, log), payment, dead_letters, stop)

        # Assert: без DLQ, один ack
        assert payment.process.await_count == 3
        dead_letters.publish.assert_not_called()
        assert log.count("ack m-1") == 1

    async def test__attempt_deadline_is_retried(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: первая попытка зависает дольше дедлайна 0.05 с, вторая успешна
        calls = 0

        async def process(payment_id: UUID) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(1)

        payment.process.side_effect = process
        payment.notify.return_value = True

        # Call
        await _run(
            FakeBroker([message(BODY, log)], stop, log),
            payment,
            dead_letters,
            stop,
            NEW_PAYMENTS_HANDLER_TIMEOUT=0.05,
        )

        # Assert
        assert calls == 2
        assert "ack m-1" in log

    async def test__exhausted_budget_stops_without_ack(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        payment.process.side_effect = errors.StorageUnavailable()

        # Call and Assert
        with pytest.raises(new_payments.RetryBudgetExhausted) as excinfo:
            await _run(
                FakeBroker([message(BODY, log)], stop, log),
                payment,
                dead_letters,
                stop,
                NEW_PAYMENTS_RETRY_BUDGET=0.05,
            )
        assert isinstance(excinfo.value.__cause__, errors.StorageUnavailable)
        assert payment.process.await_count >= 2
        assert "ack m-1" not in log
        dead_letters.publish.assert_not_called()

    async def test__stop_signal_interrupts_pause_without_ack(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup: сбой хранилища, во время паузы приходит остановка
        async def process(payment_id: UUID) -> None:
            stop.set()
            raise errors.StorageUnavailable()

        payment.process.side_effect = process

        # Call
        await _run(
            FakeBroker([message(BODY, log)], stop, log),
            payment,
            dead_letters,
            stop,
            NEW_PAYMENTS_RETRY_BACKOFF=5,
            NEW_PAYMENTS_RETRY_BACKOFF_MAX=5,
            NEW_PAYMENTS_RETRY_BUDGET=60,
        )

        # Assert: одна попытка, без ack — сообщение вернётся в очередь при закрытии канала
        assert payment.process.await_count == 1
        assert "ack m-1" not in log


class TestStop:
    @pytest.mark.parametrize("error", [RuntimeError("дефект"), _validation_error()])
    async def test__defect_stops_process_without_ack(
        self,
        payment: Mock,
        dead_letters: Mock,
        stop: asyncio.Event,
        log: list[str],
        error: Exception,
    ) -> None:
        # Setup
        payment.process.side_effect = error

        # Call and Assert
        with pytest.raises(type(error)):
            await _run(FakeBroker([message(BODY, log)], stop, log), payment, dead_letters, stop)
        assert "ack m-1" not in log
        dead_letters.publish.assert_not_called()
        assert log[-1] == "stop"


class TestAck:
    async def test__ack_failure_is_logged_not_raised(
        self, payment: Mock, dead_letters: Mock, stop: asyncio.Event, log: list[str]
    ) -> None:
        # Setup
        payment.notify.return_value = True
        item: RabbitMessage = message(BODY, log)
        cast(FakeRaw, item.raw_message).ack_error = RuntimeError("канал закрыт")

        # Call: не поднимает
        await _run(FakeBroker([item], stop, log), payment, dead_letters, stop)

        # Assert
        assert "ack m-1" not in log
        assert log[-1] == "stop"
