"""Событие платежа — факт сценария для очереди обработки (transactional outbox).

Сервис пишет событие в outbox в той же транзакции, что и платёж, поэтому опубликовано будет
ровно то, что зафиксировано. ``event_id`` рождается вместе с фактом и становится ``message_id``
сообщения: доставка — не менее одного раза, дубли гасит состояние платежа у консьюмера.
Имя — факт в прошедшем времени (``PaymentCreated``); ``type`` — дискриминатор на будущее, по
нему событие восстанавливается из хранилища.
"""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class PaymentCreated(BaseModel):
    """Платёж создан и ждёт обработки.

    Attributes:
        type: Дискриминатор события.
        event_id: Идентификатор события — ``message_id`` сообщения в очереди.
        occurred_at: Момент создания платежа.
        payment_id: Созданный платёж.
    """

    model_config = ConfigDict(frozen=True)

    type: Literal["payment_created"] = "payment_created"
    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payment_id: UUID
