"""Классификация сбоев RabbitMQ (aio-pika / aiormq) и трактовка подтверждения публикации."""

from enum import StrEnum

from aiormq.abc import DeliveredMessage
from aiormq.exceptions import AMQPConnectionError, ChannelInvalidStateError, DeliveryError
from pamqp import commands


class Confirmation(StrEnum):
    """Исход публикации с подтверждением."""

    ACKED = "acked"
    """Брокер принял сообщение и положил в очередь."""

    RETURNED = "returned"
    """Сообщение вернулось (``Basic.Return``): ни одна очередь не привязана — временно."""

    REJECTED = "rejected"
    """Брокер отказался принять сообщение (``Basic.Nack`` / ``Basic.Reject``) — постоянно."""


def is_temporary(error: BaseException) -> bool:
    """Определяет, имеет ли смысл повторить операцию после сбоя.

    Временные: обрыв или отсутствие соединения (``AMQPConnectionError``, ``ConnectionError``,
    ``OSError`` — отказ в соединении, обрыв сети), канал в закрытом состоянии после
    переподключения (``ChannelInvalidStateError``), дедлайн операции (``TimeoutError``) и
    возврат сообщения (``DeliveryError`` с кадром ``Basic.Return`` при ``mandatory=True``:
    очередь ещё не объявлена). Постоянные: ``DeliveryError`` с кадром ``Basic.Nack`` /
    ``Basic.Reject`` — брокер отверг сообщение (переполнение, политика очереди), отказ в
    доступе, отсутствие обменника, несовпадение аргументов объявленной сущности
    (``PRECONDITION_FAILED``; это ``ChannelClosed``, не ``AMQPConnectionError``) и всё остальное.

    Args:
        error: Исключение, вышедшее из операции с брокером.

    Returns:
        ``True``, если это временный сбой.
    """
    if isinstance(error, DeliveryError):
        return isinstance(error.frame, commands.Basic.Return)
    return isinstance(
        error,
        AMQPConnectionError | ChannelInvalidStateError | ConnectionError | OSError | TimeoutError,
    )


def confirmation_of(result: object) -> Confirmation:
    """Трактует результат ``broker.publish``.

    На канале с ``publisher_confirms`` aiormq сообщает о возврате и отказе исключением
    (``DeliveryError`` — см. ``is_temporary``), и сюда доходит только ``Basic.Ack``; ветки
    ``RETURNED`` и ``REJECTED`` — защита от канала с другими настройками (``on_return_raises``
    выключен — возврат приходит ``DeliveredMessage``; без подтверждений — ``None``).

    Args:
        result: Значение, которое вернул ``publish``.

    Returns:
        Исход публикации.
    """
    if result is None or isinstance(result, commands.Basic.Ack):
        return Confirmation.ACKED
    if isinstance(result, DeliveredMessage):
        return Confirmation.RETURNED
    return Confirmation.REJECTED
