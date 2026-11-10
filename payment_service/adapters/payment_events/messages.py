"""Проводная модель событий платежа — контракт с консьюмерами очередей.

Своя модель, а не событие ядра: формат очереди меняется независимо от ядра, как тела ответов
HTTP (``*Response``). Поля событий всех типов совпадают, различает их ``type`` (в теле и в
заголовке). Время — ISO 8601 с часовым поясом.
"""

from datetime import datetime
from uuid import UUID

from aio_pika.abc import HeadersType
from pydantic import BaseModel, ConfigDict

from payment_service.application import dtos

CONTENT_TYPE = "application/json"
EVENT_TYPE_HEADER = "x-event-type"


class PaymentEventMessage(BaseModel):
    """Тело сообщения о событии платежа (JSON).

    Attributes:
        event_id: Идентификатор события; он же ``message_id`` сообщения — ключ идемпотентности
            у консьюмера.
        type: Тип события: ``payment_created``, ``payment_processed``.
        occurred_at: Момент сценария, породившего событие.
        payment_id: Платёж, к которому относится событие.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    type: str
    occurred_at: datetime
    payment_id: UUID

    @classmethod
    def from_application(cls, event: dtos.AnyPaymentEvent) -> "PaymentEventMessage":
        """Строит сообщение из события ядра.

        Args:
            event: Событие платежа.

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
