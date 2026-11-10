"""Проводная модель события «платёж проведён» на входе консьюмера уведомлений.

Своя модель, как тела запросов HTTP (``*Body``): формат дублирует публикатор ``payment_events``
осознанно — адаптеры независимы, согласованность закрепляет тест шва.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from payment_service.application import dtos


class PaymentProcessedMessage(BaseModel):
    """Тело сообщения очереди проведённых платежей (JSON); лишние поля игнорируются.

    Attributes:
        event_id: Идентификатор события — ключ идемпотентности сценария (inbox).
        type: Тип события; очередь принимает только ``payment_processed``.
        payment_id: Платёж, о результате которого нужно уведомить.
        occurred_at: Момент обработки платежа; для уведомления не нужен.
    """

    model_config = ConfigDict(extra="ignore")

    event_id: UUID
    type: Literal["payment_processed"]
    payment_id: UUID
    occurred_at: datetime | None = None

    def to_application(self) -> dtos.PaymentEventInfo:
        """Переводит сообщение во вход сценария ``notify``.

        Returns:
            Событие о проведённом платеже.
        """
        return dtos.PaymentEventInfo(event_id=self.event_id, payment_id=self.payment_id)
