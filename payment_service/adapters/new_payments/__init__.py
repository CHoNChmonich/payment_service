"""Входящий адаптер очереди новых платежей ``payments.new``.

Разделение — как у HTTP-входа: ``messages`` — проводной формат (как ``*Body``), ``handler`` —
сообщение → DTO → порт ``interfaces.Payment`` (как ``Controller``; ошибка разбора —
``InvalidMessage``, как ``RequestValidationError``), ``policy`` — исключение → действие по
категориям ошибок ядра и код причины для DLQ (как ``error_handlers``), ``settings`` — очередь,
DLQ и дедлайны. Цикл, повторы, подтверждения и запись в DLQ (роль сервера) — библиотека
``rabbitmq``; консьюмер собирает composition root. Об исходящем адаптере событий
(``payment_events``) не знает.
"""

from .handler import InvalidMessage, NewPaymentsHandler
from .messages import PaymentCreatedMessage
from .policy import action_for, error_code
from .settings import Settings

__all__ = [
    "InvalidMessage",
    "NewPaymentsHandler",
    "PaymentCreatedMessage",
    "Settings",
    "action_for",
    "error_code",
]
