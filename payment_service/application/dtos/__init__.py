"""Модели обмена ядра с внешними слоями (Pydantic).

``NewPaymentInfo`` — вход сценария создания, ``PaymentEventInfo`` — вход сценариев, вызываемых
сообщением (``process``, ``notify``), ``PaymentInfo`` — выход сценариев (строится из сущности
через ``model_validate``), ``PaymentCreated`` / ``PaymentProcessed`` — события платежа для outbox
(``payment_event``). Между слоями передаются только они и сущности, не dict/list. Ограничения
полей (``base``) общие с HTTP-швом.
"""

from .base import (
    IDEMPOTENCY_KEY_PATTERN,
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
from .payment import NewPaymentInfo, PaymentEventInfo, PaymentInfo
from .payment_event import (
    PAYMENT_EVENT_TYPES,
    AnyPaymentEvent,
    PaymentCreated,
    PaymentEvent,
    PaymentEventType,
    PaymentProcessed,
    RelayResult,
)

__all__ = [
    "IDEMPOTENCY_KEY_PATTERN",
    "MAX_DESCRIPTION_LENGTH",
    "MAX_IDEMPOTENCY_KEY_LENGTH",
    "MAX_METADATA_BYTES",
    "MAX_WEBHOOK_URL_LENGTH",
    "PAYMENT_EVENT_TYPES",
    "Amount",
    "AnyPaymentEvent",
    "Description",
    "IdempotencyKey",
    "Metadata",
    "NewPaymentInfo",
    "PaymentCreated",
    "PaymentEvent",
    "PaymentEventInfo",
    "PaymentEventType",
    "PaymentInfo",
    "PaymentProcessed",
    "RelayResult",
    "WebhookUrl",
]
