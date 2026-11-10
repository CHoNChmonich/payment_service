"""Исходящий адаптер событий платежей: публикация в RabbitMQ (FastStream).

Реализует порт ``interfaces.EventPublisher``. ``messages`` — проводной формат очереди (свой,
как тела ответов HTTP), ``publisher`` — публикация с подтверждением и переводом сбоев:
временные — ``errors.BrokerUnavailable``, постоянные — ``EventPublishingFailed``. О входящем
адаптере сообщений (``new_payments``) не знает; ``rabbitmq`` использует как библиотеку.
"""

from .messages import CONTENT_TYPE, PaymentCreatedMessage
from .publisher import EventPublisher, EventPublishingFailed
from .settings import Settings

__all__ = [
    "CONTENT_TYPE",
    "EventPublisher",
    "EventPublishingFailed",
    "PaymentCreatedMessage",
    "Settings",
]
