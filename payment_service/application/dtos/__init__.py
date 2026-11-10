"""Модели обмена ядра с внешними слоями (Pydantic).

``NewPaymentInfo`` — вход сценария создания, ``PaymentInfo`` — выход сценариев (строится из
сущности через ``model_validate``), ``PaymentCreated`` — событие платежа для outbox
(``payment_event``). Между слоями передаются только они и сущности, не dict/list. Ограничения
полей (``base``) общие с HTTP-швом.
"""

from .base import (
    API_KEY_PATTERN,
    IDEMPOTENCY_KEY_PATTERN,
    MAX_API_KEY_LENGTH,
    MAX_DESCRIPTION_LENGTH,
    MAX_IDEMPOTENCY_KEY_LENGTH,
    MAX_METADATA_BYTES,
    MAX_WEBHOOK_URL_LENGTH,
    Amount,
    Description,
    IdempotencyKey,
    Metadata,
    WebhookUrl,
)
from .payment import NewPaymentInfo, PaymentInfo
from .payment_event import PaymentCreated

__all__ = [
    "API_KEY_PATTERN",
    "IDEMPOTENCY_KEY_PATTERN",
    "MAX_API_KEY_LENGTH",
    "MAX_DESCRIPTION_LENGTH",
    "MAX_IDEMPOTENCY_KEY_LENGTH",
    "MAX_METADATA_BYTES",
    "MAX_WEBHOOK_URL_LENGTH",
    "Amount",
    "Description",
    "IdempotencyKey",
    "Metadata",
    "NewPaymentInfo",
    "PaymentCreated",
    "PaymentInfo",
    "WebhookUrl",
]
