import asyncio
from typing import Any, cast

import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import ChannelAccessRefused, ConnectionClosed, PublishError
from faststream.rabbit import RabbitExchange
from pamqp import commands
from pamqp.header import ContentHeader

from payment_service.adapters import new_payments
from payment_service.application import errors

from .support import FakeBroker, message, settings

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
BODY = b'{"payment_id": "x"}'
SETTINGS = settings()


def _dead_letters(
    replies: list[object] | None = None,
) -> tuple[new_payments.DeadLetters, FakeBroker]:
    broker = FakeBroker([], asyncio.Event(), [], replies=replies)
    return new_payments.DeadLetters(SETTINGS, cast(Any, broker)), broker


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
class TestPublish:
    async def test__publishes_original_message_with_reason(self) -> None:
        # Setup
        dead_letters, broker = _dead_letters()
        original = message(
            BODY,
            [],
            message_id="m-7",
            headers={"x-event-type": "payment_created", "x-error": "old"},
        )
        error = errors.WebhookUnavailable(url="http://sink/hook")

        # Call
        await dead_letters.publish(original, error, attempts=3)

        # Assert
        (published,) = broker.published
        exchange = published["exchange"]
        assert isinstance(exchange, RabbitExchange)
        assert exchange.name == "payments.dlx"
        assert published["routing_key"] == "payments.new.dlq"
        assert published["message"] == BODY
        assert published["message_id"] == "m-7"
        assert published["content_type"] == "application/json"
        assert (published["mandatory"], published["persist"], published["timeout"]) == (
            True,
            True,
            1,
        )
        headers = cast(dict[str, object], published["headers"])
        assert headers["x-event-type"] == "payment_created"
        assert headers["x-error-code"] == "webhook_unavailable"
        assert headers["x-error"] == "Получатель webhook недоступен по адресу http://sink/hook"
        assert headers["x-attempts"] == 3
        assert headers["x-original-exchange"] == "payments"
        assert headers["x-original-routing-key"] == "payments.new"
        assert isinstance(headers["x-failed-at"], str)

    async def test__non_core_error_uses_class_name(self) -> None:
        # Setup
        dead_letters, broker = _dead_letters()

        # Call
        await dead_letters.publish(
            message(BODY, []), new_payments.InvalidMessage("не по контракту"), attempts=1
        )

        # Assert
        headers = cast(dict[str, object], broker.published[0]["headers"])
        assert headers["x-error-code"] == "InvalidMessage"
        assert headers["x-error"] == "не по контракту"

    async def test__connection_loss_is_temporary(self) -> None:
        # Setup
        dead_letters, _ = _dead_letters([ConnectionClosed(320, "CONNECTION_FORCED")])

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterUnavailable):
            await dead_letters.publish(message(BODY, []), RuntimeError("x"), attempts=1)

    async def test__returned_message_is_temporary(self) -> None:
        # Setup
        dead_letters, _ = _dead_letters([_returned()])

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterUnavailable, match="возвращено"):
            await dead_letters.publish(message(BODY, []), RuntimeError("x"), attempts=1)

    async def test__publish_error_on_return_is_temporary(self) -> None:
        # Setup
        returned = _returned()
        dead_letters, _ = _dead_letters([PublishError(returned, returned.delivery)])

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterUnavailable):
            await dead_letters.publish(message(BODY, []), RuntimeError("x"), attempts=1)

    async def test__access_refused_is_permanent(self) -> None:
        # Setup
        dead_letters, _ = _dead_letters([ChannelAccessRefused(403, "ACCESS_REFUSED")])

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterFailed):
            await dead_letters.publish(message(BODY, []), RuntimeError("x"), attempts=1)

    async def test__nack_is_permanent(self) -> None:
        # Setup
        dead_letters, _ = _dead_letters([commands.Basic.Nack(delivery_tag=1)])

        # Call and Assert
        with pytest.raises(new_payments.DeadLetterFailed, match="отверг"):
            await dead_letters.publish(message(BODY, []), RuntimeError("x"), attempts=1)
