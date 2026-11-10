import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import ChannelAccessRefused, ConnectionClosed, PublishError
from faststream.rabbit import RabbitExchange
from pamqp import commands
from pamqp.header import ContentHeader

from payment_service.adapters import payment_events
from payment_service.application import dtos, errors

from .fake_broker import FakeBroker

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
SETTINGS = payment_events.Settings.model_validate(
    {"RABBITMQ_URL": "amqp://guest:guest@rabbitmq:5672/", "PAYMENT_EVENTS_PUBLISH_TIMEOUT": 3}
)
EVENT = dtos.PaymentCreated(
    event_id=UUID("11111111-1111-1111-1111-111111111111"),
    occurred_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    payment_id=UUID("22222222-2222-2222-2222-222222222222"),
)
OTHER = dtos.PaymentCreated(
    event_id=UUID("33333333-3333-3333-3333-333333333333"),
    occurred_at=datetime(2026, 11, 10, 12, 1, tzinfo=UTC),
    payment_id=UUID("44444444-4444-4444-4444-444444444444"),
)


async def _publisher(broker: FakeBroker) -> payment_events.EventPublisher:
    urls: list[str] = []

    def factory(url: str) -> FakeBroker:
        urls.append(url)
        return broker

    publisher = payment_events.EventPublisher(SETTINGS, broker_factory=factory)  # type: ignore[arg-type]
    assert urls == ["amqp://guest:guest@rabbitmq:5672/"]
    await publisher.aopen()
    return publisher


def _returned() -> DeliveredMessage:
    return DeliveredMessage(
        delivery=commands.Basic.Return(reply_code=312, reply_text="NO_ROUTE"),
        header=ContentHeader(),
        body=b"",
        channel=None,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestAopen:
    async def test__starts_and_declares_exchange(self) -> None:
        # Setup
        broker = FakeBroker()

        # Call
        await _publisher(broker)

        # Assert
        assert broker.log == ["start", "declare_exchange payments"]

    async def test__broker_unreachable_is_startup_error(self) -> None:
        # Setup
        broker = FakeBroker()
        broker.start_error = ConnectionRefusedError("отказ")
        publisher = payment_events.EventPublisher(SETTINGS, broker_factory=lambda _: broker)  # type: ignore[arg-type, return-value]

        # Call and Assert
        with pytest.raises(RuntimeError, match="RabbitMQ недоступен") as excinfo:
            await publisher.aopen()

        assert isinstance(excinfo.value.__cause__, ConnectionRefusedError)


class TestPublish:
    async def test__publish(self) -> None:
        # Setup
        broker = FakeBroker()
        publisher = await _publisher(broker)

        # Call
        await publisher.publish([EVENT, OTHER])

        # Assert: по сообщению на событие; persist, mandatory, message_id = event_id, дедлайн
        assert [p["message_id"] for p in broker.published] == [
            str(EVENT.event_id),
            str(OTHER.event_id),
        ]
        first = broker.published[0]
        exchange = first["exchange"]
        assert isinstance(exchange, RabbitExchange)
        assert exchange.name == "payments"
        assert first["routing_key"] == "payments.new"
        assert (first["mandatory"], first["persist"], first["timeout"]) == (True, True, 3)
        assert first["content_type"] == "application/json"
        assert first["headers"] == {"x-event-type": "payment_created"}
        body = first["message"]
        assert isinstance(body, bytes)
        assert json.loads(body) == {
            "event_id": str(EVENT.event_id),
            "type": "payment_created",
            "occurred_at": "2026-11-10T12:00:00Z",
            "payment_id": str(EVENT.payment_id),
        }

    async def test__empty_batch(self) -> None:
        # Setup
        broker = FakeBroker()
        publisher = await _publisher(broker)

        # Call
        await publisher.publish([])

        # Assert
        assert broker.published == []

    async def test__before_open(self) -> None:
        # Call and Assert
        publisher = payment_events.EventPublisher(SETTINGS, broker_factory=lambda _: FakeBroker())  # type: ignore[arg-type, return-value]
        with pytest.raises(RuntimeError, match="aopen"):
            await publisher.publish([EVENT])

    @pytest.mark.parametrize(
        "reply",
        [
            ConnectionClosed(320, "CONNECTION_FORCED"),
            TimeoutError(),
            PublishError(_returned(), commands.Basic.Return(reply_code=312, reply_text="NO_ROUTE")),
            _returned(),
        ],
    )
    async def test__temporary_failure(self, reply: object) -> None:
        # Setup: вторая публикация не проходит — первая успела
        broker = FakeBroker(replies=[commands.Basic.Ack(delivery_tag=1), reply])
        publisher = await _publisher(broker)

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable) as excinfo:
            await publisher.publish([EVENT, OTHER])

        assert excinfo.value.__cause__ is not None
        assert len(broker.published) == 2

    @pytest.mark.parametrize(
        "reply",
        [
            ChannelAccessRefused(403, "ACCESS_REFUSED"),
            commands.Basic.Nack(delivery_tag=1),
            commands.Basic.Reject(delivery_tag=1),
        ],
    )
    async def test__permanent_failure_names_event(self, reply: object) -> None:
        # Setup
        broker = FakeBroker(replies=[commands.Basic.Ack(delivery_tag=1), reply])
        publisher = await _publisher(broker)

        # Call and Assert: по тексту ошибки строку outbox можно найти и убрать
        with pytest.raises(payment_events.EventPublishingFailed) as excinfo:
            await publisher.publish([EVENT, OTHER])

        assert f"событие {OTHER.event_id} платежа {OTHER.payment_id}" in str(excinfo.value)

    async def test__no_confirmation_is_success(self) -> None:
        # Setup
        broker = FakeBroker(replies=[None])
        publisher = await _publisher(broker)

        # Call and Assert: канал без подтверждений ничего не возвращает — не ошибка
        await publisher.publish([EVENT])


class TestAclose:
    async def test__stops_broker_once(self) -> None:
        # Setup
        broker = FakeBroker()
        publisher = await _publisher(broker)

        # Call
        await publisher.aclose()
        await publisher.aclose()

        # Assert
        assert broker.log == ["start", "declare_exchange payments", "stop"]

    async def test__before_open(self) -> None:
        # Setup
        broker = FakeBroker()
        publisher = payment_events.EventPublisher(SETTINGS, broker_factory=lambda _: broker)  # type: ignore[arg-type, return-value]

        # Call and Assert: остановка процесса, у которого запуск не дошёл до брокера
        await publisher.aclose()
        assert broker.log == []


class TestSettings:
    def test__url_hidden_in_repr(self) -> None:
        # Call
        settings = payment_events.Settings.model_validate({"RABBITMQ_URL": "amqp://u:p@h/"})

        # Assert: URL содержит пароль брокера
        assert "amqp://u:p@h/" not in repr(settings)
