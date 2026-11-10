"""DTO платежа: вход сценария создания и выход всех сценариев."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from payment_service.application.entities import Currency, PaymentStatus

from .base import Amount, Description, IdempotencyKey, Metadata, WebhookUrl


class NewPaymentInfo(BaseModel):
    """Данные для создания платежа.

    Attributes:
        idempotency_key: Ключ идемпотентности запроса из заголовка ``Idempotency-Key``.
        amount: Сумма платежа.
        currency: Валюта платежа.
        description: Назначение платежа; может быть пустым.
        metadata: Произвольные данные клиента, сохраняются как есть и возвращаются в webhook.
        webhook_url: Адрес, на который отправляется уведомление о результате.
    """

    idempotency_key: IdempotencyKey
    amount: Amount
    currency: Currency
    description: Description = ""
    metadata: Metadata = Field(default_factory=dict)
    webhook_url: WebhookUrl


class PaymentInfo(BaseModel):
    """Платёж на выходе сценария; строится из сущности через ``model_validate``.

    Attributes:
        id: Идентификатор платежа.
        idempotency_key: Ключ идемпотентности запроса на создание.
        amount: Сумма платежа.
        currency: Валюта платежа.
        description: Назначение платежа.
        metadata: Данные клиента.
        webhook_url: Адрес уведомления о результате.
        status: Текущий статус.
        created_at: Момент создания.
        processed_at: Момент перехода в финальный статус; ``None``, пока платёж в обработке.
        notified_at: Момент доставки webhook; ``None``, пока уведомление не доставлено.
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    notified_at: datetime | None
