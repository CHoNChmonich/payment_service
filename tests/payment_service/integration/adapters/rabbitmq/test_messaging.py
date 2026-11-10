"""Шов публикатор → брокер → консьюмеры: маршрутизация по типу события, подтверждение, DLQ,
возврат в очередь."""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractIncomingMessage
from aiormq.exceptions import ChannelPreconditionFailed

from payment_service.adapters import new_payments, payment_events, processed_payments, rabbitmq
from payment_service.application import dtos, errors, interfaces

from .conftest import Names

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
TIMEOUT = 15


def _event() -> dtos.PaymentCreated:
    return dtos.PaymentCreated(event_id=uuid4(), occurred_at=datetime.now(UTC), payment_id=uuid4())


def _processed_event() -> dtos.PaymentProcessed:
    return dtos.PaymentProcessed(
        event_id=uuid4(), occurred_at=datetime.now(UTC), payment_id=uuid4()
    )


def _info(event: dtos.AnyPaymentEvent) -> dtos.PaymentEventInfo:
    return dtos.PaymentEventInfo(event_id=event.event_id, payment_id=event.payment_id)


@pytest.fixture(scope="function")
async def publisher(
    publisher_settings: payment_events.Settings,
) -> AsyncIterator[payment_events.EventPublisher]:
    publisher = payment_events.EventPublisher(publisher_settings)
    await publisher.aopen()

    yield publisher

    await publisher.aclose()


@pytest.fixture(scope="function")
async def declared(
    consumer_settings: new_payments.Settings, processed_settings: processed_payments.Settings
) -> None:
    # Топология объявляется до публикации: иначе сообщение с mandatory вернётся (очереди нет).
    # Каждая очередь — дважды: повторное объявление идемпотентно; общие обменник и DLX двумя
    # консьюмерами объявляются одинаково
    broker = rabbitmq.create_broker(
        consumer_settings.RABBITMQ_URL.get_secret_value(), graceful_timeout=5
    )
    await broker.connect()
    topologies = [
        consumer_settings.consumer_config.topology,
        processed_settings.consumer_config.topology,
    ]
    try:
        for topology in topologies * 2:
            await rabbitmq.declare(broker, topology)
    finally:
        await broker.stop()


async def _run_consumer(
    settings: new_payments.Settings, payment: Mock, until: Awaitable[object]
) -> None:
    await _run_queue_consumer(
        settings.RABBITMQ_URL.get_secret_value(),
        settings.consumer_config,
        new_payments.NewPaymentsHandler(payment).handle,
        new_payments.action_for,
        new_payments.error_code,
        until,
    )


async def _run_notifier(
    settings: processed_payments.Settings, payment: Mock, until: Awaitable[object]
) -> None:
    await _run_queue_consumer(
        settings.RABBITMQ_URL.get_secret_value(),
        settings.consumer_config,
        processed_payments.ProcessedPaymentsHandler(payment).handle,
        processed_payments.action_for,
        processed_payments.error_code,
        until,
    )


async def _run_queue_consumer(
    url: str,
    config: rabbitmq.ConsumerConfig,
    handler: rabbitmq.Handler,
    action_for: rabbitmq.Policy,
    error_code: rabbitmq.ErrorCode,
    until: Awaitable[object],
) -> None:
    broker = rabbitmq.create_broker(url, graceful_timeout=5)
    consumer = rabbitmq.QueueConsumer(
        handler=handler,
        action_for=action_for,
        dead_letters=rabbitmq.DeadLetters(config.dead_letter_target, broker, error_code),
        config=config,
        broker=broker,
    )
    stop = asyncio.Event()
    stopper = asyncio.ensure_future(_set_after(until, stop))
    try:
        async with asyncio.timeout(TIMEOUT):
            await consumer.run(stop)
    finally:
        stopper.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stopper


async def _set_after(until: Awaitable[object], stop: asyncio.Event) -> None:
    try:
        await until
    finally:
        stop.set()


def _handled(payment: Mock, method: str, outcome: Exception | object = None) -> asyncio.Event:
    handled = asyncio.Event()

    async def side_effect(event: dtos.PaymentEventInfo) -> object:
        handled.set()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    getattr(payment, method).side_effect = side_effect
    return handled


async def _message_count(url: str, queue: str) -> int:
    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        return declared.declaration_result.message_count or 0


async def _get_message(url: str, queue: str) -> AbstractIncomingMessage:
    connection = await aio_pika.connect_robust(url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        message = await declared.get(no_ack=True, fail=True, timeout=TIMEOUT)
        assert message is not None
        return message


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPublisherToConsumer:
    @pytest.mark.usefixtures("declared")
    async def test__event_is_delivered_processed_and_acked(
        self,
        publisher: payment_events.EventPublisher,
        consumer_settings: new_payments.Settings,
        names: Names,
    ) -> None:
        # Setup
        event = _event()
        payment = Mock(interfaces.Payment)
        handled = _handled(payment, "process", outcome=True)

        # Call
        await publisher.publish([event])
        await _run_consumer(consumer_settings, payment, handled.wait())

        # Assert: сценарий получил событие, сообщение подтверждено — очередь пуста
        payment.process.assert_awaited_once_with(_info(event))
        payment.notify.assert_not_called()
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        assert await _message_count(url, names.queue) == 0
        assert await _message_count(url, names.dlq) == 0

    @pytest.mark.usefixtures("declared")
    async def test__events_are_routed_by_type(
        self,
        publisher: payment_events.EventPublisher,
        consumer_settings: new_payments.Settings,
        processed_settings: processed_payments.Settings,
        names: Names,
    ) -> None:
        # Setup
        created, processed = _event(), _processed_event()
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        payment = Mock(interfaces.Payment)
        handled = _handled(payment, "notify", outcome=True)

        # Call
        await publisher.publish([created, processed])
        counts = (
            await _message_count(url, names.queue),
            await _message_count(url, names.processed_queue),
        )
        await _run_notifier(processed_settings, payment, handled.wait())

        # Assert: по сообщению в очередь своего типа; консьюмер уведомлений получил своё
        assert counts == (1, 1)
        payment.notify.assert_awaited_once_with(_info(processed))
        payment.process.assert_not_called()
        assert await _message_count(url, names.processed_queue) == 0
        assert await _message_count(url, names.queue) == 1
        assert await _message_count(url, names.processed_dlq) == 0

    @pytest.mark.usefixtures("declared")
    async def test__rejected_notification_lands_in_its_dlq(
        self,
        publisher: payment_events.EventPublisher,
        processed_settings: processed_payments.Settings,
        names: Names,
    ) -> None:
        # Setup
        event = _processed_event()
        payment = Mock(interfaces.Payment)
        handled = _handled(
            payment, "notify", outcome=errors.WebhookRejected(url="u", status_code=410)
        )

        # Call
        await publisher.publish([event])
        await _run_notifier(processed_settings, payment, handled.wait())

        # Assert: своя DLQ уведомлений, DLQ новых платежей не тронута
        url = processed_settings.RABBITMQ_URL.get_secret_value()
        dead = await _get_message(url, names.processed_dlq)
        assert dead.message_id == str(event.event_id)
        assert dead.headers["x-event-type"] == "payment_processed"
        assert dead.headers["x-error-code"] == "webhook_rejected"
        assert dead.headers["x-original-routing-key"] == names.processed_routing_key
        assert await _message_count(url, names.dlq) == 0

    @pytest.mark.usefixtures("declared")
    async def test__rejected_message_lands_in_dlq_with_reason(
        self,
        publisher: payment_events.EventPublisher,
        consumer_settings: new_payments.Settings,
        names: Names,
    ) -> None:
        # Setup
        event = _event()
        payment = Mock(interfaces.Payment)
        handled = _handled(payment, "process", outcome=errors.PaymentNotFound(id=event.payment_id))

        # Call
        await publisher.publish([event])
        await _run_consumer(consumer_settings, payment, handled.wait())

        # Assert: исходное тело и свойства плюс причина; рабочая очередь пуста
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        dead = await _get_message(url, names.dlq)
        assert json.loads(dead.body)["payment_id"] == str(event.payment_id)
        assert dead.message_id == str(event.event_id)
        assert dead.content_type == "application/json"
        assert dead.headers["x-event-type"] == "payment_created"
        assert dead.headers["x-error-code"] == "payment_not_found"
        assert dead.headers["x-attempts"] == 1
        assert dead.headers["x-original-exchange"] == names.exchange
        assert dead.headers["x-original-routing-key"] == names.routing_key
        assert await _message_count(url, names.queue) == 0

    @pytest.mark.usefixtures("declared")
    async def test__defect_stops_consumer_and_message_returns_to_queue(
        self,
        publisher: payment_events.EventPublisher,
        consumer_settings: new_payments.Settings,
        names: Names,
    ) -> None:
        # Setup
        event = _event()
        payment = Mock(interfaces.Payment)
        _handled(payment, "process", outcome=RuntimeError("дефект"))
        never = asyncio.Event()

        # Call and Assert: run завершается дефектом, сообщение не подтверждено
        with pytest.raises(RuntimeError, match="дефект"):
            await publisher.publish([event])
            await _run_consumer(consumer_settings, payment, never.wait())
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        assert await _message_count(url, names.queue) == 1
        assert await _message_count(url, names.dlq) == 0

    @pytest.mark.usefixtures("declared")
    async def test__infrastructure_outage_is_retried_in_place(
        self,
        publisher: payment_events.EventPublisher,
        consumer_settings: new_payments.Settings,
        names: Names,
    ) -> None:
        # Setup: первая попытка — сбой хранилища, вторая успешна
        event = _event()
        payment = Mock(interfaces.Payment)
        handled = asyncio.Event()
        outcomes: list[Exception | bool] = [errors.StorageUnavailable(), True]

        async def process(info: dtos.PaymentEventInfo) -> bool:
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            handled.set()
            return outcome

        payment.process.side_effect = process

        # Call
        await publisher.publish([event])
        await _run_consumer(consumer_settings, payment, handled.wait())

        # Assert
        assert payment.process.await_count == 2
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        assert await _message_count(url, names.queue) == 0
        assert await _message_count(url, names.dlq) == 0


class TestPublisher:
    async def test__publish_without_bound_queue_is_broker_unavailable(
        self, publisher: payment_events.EventPublisher
    ) -> None:
        # Setup: обменник объявлен aopen, очереди к нему не привязано — mandatory-возврат

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable):
            await publisher.publish([_event()])


class TestDeclare:
    async def test__declare_after_mismatched_arguments_is_permanent_error(
        self, consumer_settings: new_payments.Settings, names: Names
    ) -> None:
        # Setup: очередь с другим DLX уже есть
        url = consumer_settings.RABBITMQ_URL.get_secret_value()
        connection = await aio_pika.connect_robust(url)
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue(
                names.queue, durable=True, arguments={"x-dead-letter-exchange": "other"}
            )
        broker = rabbitmq.create_broker(url, graceful_timeout=5)
        await broker.connect()

        # Call and Assert: PRECONDITION_FAILED — не временный сбой
        try:
            with pytest.raises(ChannelPreconditionFailed) as excinfo:
                await rabbitmq.declare(broker, consumer_settings.consumer_config.topology)
            assert not rabbitmq.is_temporary(excinfo.value)
        finally:
            await broker.stop()
