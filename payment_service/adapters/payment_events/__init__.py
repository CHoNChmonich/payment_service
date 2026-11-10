"""Исходящий адаптер событий платежей: публикация в RabbitMQ (FastStream).

Реализует порт ``interfaces.EventPublisher``. ``messages`` — проводной формат очереди (свой,
как тела ответов HTTP; тип события — в теле и заголовке), ``publisher`` — публикация с
ключом маршрутизации по типу события, подтверждением и переводом сбоев:
временные — ``errors.BrokerUnavailable``, постоянные — ``EventPublishingFailed``. О входящих
адаптерах сообщений (``new_payments``, ``processed_payments``) не знает; ``rabbitmq`` использует
как библиотеку.
"""

from .messages import CONTENT_TYPE, PaymentEventMessage
from .publisher import EventPublisher, EventPublishingFailed
from .settings import Settings

__all__ = [
    "CONTENT_TYPE",
    "EventPublisher",
    "EventPublishingFailed",
    "PaymentEventMessage",
    "Settings",
]
