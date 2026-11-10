"""Тело webhook — контракт с получателем уведомлений.

Своя модель, как тела ответов HTTP (``*Response``): формат уведомления меняется независимо от
ядра. Деньги — строкой с двумя знаками, время — ISO 8601 с часовым поясом.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from payment_service.application import dtos

CONTENT_TYPE = "application/json"
PAYMENT_ID_HEADER = "X-Payment-Id"


class WebhookMessage(BaseModel):
    """Тело уведомления о результате платежа (JSON).

    Attributes:
        payment_id: Платёж; дублируется заголовком ``X-Payment-Id`` для дедупликации у
            получателя — уведомление может прийти дважды (доставка не менее одного раза).
        status: Итоговый статус: ``succeeded`` или ``failed``.
        amount: Сумма строкой с двумя знаками.
        currency: Валюта.
        description: Назначение платежа.
        metadata: Метаданные клиента как были переданы при создании.
        created_at: Момент создания платежа.
        processed_at: Момент получения результата от шлюза.
    """

    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    status: str
    amount: str
    currency: str
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_application(cls, payment: dtos.PaymentInfo) -> "WebhookMessage":
        """Строит уведомление из DTO ядра.

        Args:
            payment: Обработанный платёж.

        Returns:
            Тело уведомления.
        """
        return cls(
            payment_id=payment.id,
            status=payment.status.value,
            amount=f"{payment.amount:.2f}",
            currency=payment.currency.value,
            description=payment.description,
            metadata=payment.metadata,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )

    def body(self) -> bytes:
        """Тело запроса: JSON в UTF-8.

        Returns:
            Байты тела.
        """
        return self.model_dump_json().encode()
