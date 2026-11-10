"""Проводная модель события «платёж создан» на входе консьюмера.

Своя модель, как тела запросов HTTP (``*Body``): формат дублирует публикатор ``payment_events``
осознанно — адаптеры независимы, согласованность закрепляет интеграционный тест шва.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class PaymentCreatedMessage(BaseModel):
    """Тело сообщения очереди новых платежей (JSON); лишние поля игнорируются.

    Attributes:
        event_id: Идентификатор события.
        type: Тип события; очередь принимает только ``payment_created``.
        payment_id: Платёж, который нужно обработать.
        occurred_at: Момент создания платежа; для обработки не нужен.
    """

    model_config = ConfigDict(extra="ignore")

    event_id: UUID
    type: Literal["payment_created"]
    payment_id: UUID
    occurred_at: datetime | None = None

    def to_application(self) -> UUID:
        """Переводит сообщение во вход сценариев ``process`` и ``notify``.

        Returns:
            Идентификатор платежа.
        """
        return self.payment_id
