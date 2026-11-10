"""Входящий адаптер новых платежей: консьюмер RabbitMQ (FastStream) с DLQ.

Разделение — как у HTTP-входа: ``messages`` — проводной формат (как ``*Body``), ``handler`` —
сообщение → идентификатор → порт ``interfaces.Payment`` (как ``Controller``; ошибка разбора —
``InvalidMessage``, как ``RequestValidationError``), ``policy`` — исключение → действие по
категориям ошибок ядра (как ``error_handlers``), ``consumer`` — подписка, повторы и
подтверждения (как сервер), ``dead_letters`` — запись в DLQ, ``topology`` — очереди и DLQ. Об
исходящем адаптере событий (``payment_events``) не знает; ``rabbitmq`` использует как библиотеку.
"""

from .consumer import NewPaymentsConsumer, RetryBudgetExhausted
from .dead_letters import DeadLetterFailed, DeadLetters, DeadLetterUnavailable
from .handler import InvalidMessage, NewPaymentsHandler
from .messages import PaymentCreatedMessage
from .policy import Action, action_for
from .settings import Settings
from .topology import QueueTopology, declare

__all__ = [
    "Action",
    "DeadLetterFailed",
    "DeadLetterUnavailable",
    "DeadLetters",
    "InvalidMessage",
    "NewPaymentsConsumer",
    "NewPaymentsHandler",
    "PaymentCreatedMessage",
    "QueueTopology",
    "RetryBudgetExhausted",
    "Settings",
    "action_for",
    "declare",
]
