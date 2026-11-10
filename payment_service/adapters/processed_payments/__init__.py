"""Входящий адаптер очереди проведённых платежей ``payments.processed``: уведомление клиента.

Раскладка — как у ``new_payments``: ``messages`` — проводной формат, ``handler`` — сообщение →
DTO → сценарий ``notify`` порта ``interfaces.Payment``, ``policy`` — исключение → действие и код
причины для DLQ, ``settings`` — очередь, DLQ и дедлайны. Цикл, повторы, подтверждения и DLQ —
библиотека ``rabbitmq``; консьюмер собирает composition root процесса ``notifier``.
"""

from .handler import InvalidMessage, ProcessedPaymentsHandler
from .messages import PaymentProcessedMessage
from .policy import action_for, error_code
from .settings import Settings

__all__ = [
    "InvalidMessage",
    "PaymentProcessedMessage",
    "ProcessedPaymentsHandler",
    "Settings",
    "action_for",
    "error_code",
]
