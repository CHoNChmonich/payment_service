"""Проводная модель события «платёж создан» — контракт с консьюмером очереди.

Своя модель, а не событие ядра: формат очереди меняется независимо от ядра, как тела ответов
HTTP (``*Response``). Время — ISO 8601 с часовым поясом.
"""

from datetime import datetime
from uuid import UUID

from aio_pika.abc import HeadersType
from pydantic import BaseModel, ConfigDict

from payment_service.application import dtos

CONTENT_TYPE = "application/json"
EVENT_TYPE_HEADER = "x-event-type"


class PaymentCreatedMessage(BaseModel):
    """Тело сообщения о созданном платеже (JSON).

    Attributes:
        event_id: Идентификатор события; он же ``message_id`` сообщения.
        type: Тип события — ``payment_created``.
        occurred_at: Момент создания платежа.
        payment_id: Платёж, который нужно обработать.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    type: str
    occurred_at: datetime
    payment_id: UUID

    @classmethod
    def from_application(cls, event: dtos.PaymentCreated) -> "PaymentCreatedMessage":
        """Строит сообщение из события ядра.

        Args:
            event: Событие «платёж создан».

        Returns:
            Проводная модель события.
        """
        return cls(
            event_id=event.event_id,
            type=event.type,
            occurred_at=event.occurred_at,
            payment_id=event.payment_id,
        )

    def body(self) -> bytes:
        """Тело сообщения: JSON в UTF-8.

        Returns:
            Байты тела.
        """
        return self.model_dump_json().encode()

    def headers(self) -> HeadersType:
        """Заголовки AMQP: тип события — фильтрация и разбор без чтения тела.

        Returns:
            Заголовки сообщения.
        """
        return {EVENT_TYPE_HEADER: self.type}
