import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import (
    AMQPConnectionError,
    ChannelAccessRefused,
    ChannelInvalidStateError,
    ChannelNotFoundEntity,
    ChannelPreconditionFailed,
    ConnectionClosed,
    DeliveryError,
    PublishError,
)
from pamqp import commands
from pamqp.header import ContentHeader

from payment_service.adapters import rabbitmq


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
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
class TestIsTemporary:
    @pytest.mark.parametrize(
        "error",
        [
            AMQPConnectionError(),
            ConnectionClosed(320, "CONNECTION_FORCED"),
            ConnectionRefusedError(),
            ConnectionResetError(),
            OSError("сеть недоступна"),
            TimeoutError(),
            ChannelInvalidStateError("channel closed"),
            PublishError(_returned(), commands.Basic.Return(reply_code=312, reply_text="NO_ROUTE")),
        ],
    )
    def test__temporary(self, error: BaseException) -> None:
        # Call and Assert
        assert rabbitmq.is_temporary(error) is True

    @pytest.mark.parametrize(
        "error",
        [
            ChannelAccessRefused(403, "ACCESS_REFUSED"),
            ChannelNotFoundEntity(404, "NOT_FOUND"),
            ChannelPreconditionFailed(406, "PRECONDITION_FAILED"),
            DeliveryError(None, commands.Basic.Nack(delivery_tag=1)),
            DeliveryError(None, commands.Basic.Reject(delivery_tag=1)),
            RuntimeError("дефект"),
            ValueError("плохой ключ маршрутизации"),
        ],
    )
    def test__permanent(self, error: BaseException) -> None:
        # Call and Assert
        assert rabbitmq.is_temporary(error) is False
