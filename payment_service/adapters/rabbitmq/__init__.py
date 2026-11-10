"""Общее для адаптеров RabbitMQ: брокер, топология, классификация сбоев, цикл консьюмера и DLQ.

Библиотека, а не адаптер: портов не реализует, адаптеры и ядро не импортирует. Её используют
``payment_events`` (выход: публикация событий) и входящие адаптеры очередей (``new_payments``,
``processed_payments``), чтобы настройки канала, правило «временный сбой или постоянный»,
трактовка подтверждения публикации и протокол подтверждения доставки (цикл, повторы, DLQ)
жили в одном месте. Адаптеры входа дают консьюмеру свой обработчик, политику ошибок и код
причины для DLQ; друг о друге адаптеры не знают.
"""

from .broker import create_broker, logger_levels
from .consumer import (
    Action,
    ConsumerConfig,
    Handler,
    Policy,
    QueueConsumer,
    RetryBudgetExhausted,
)
from .dead_letters import (
    DeadLetterFailed,
    DeadLetters,
    DeadLetterUnavailable,
    ErrorCode,
)
from .errors import Confirmation, confirmation_of, is_temporary
from .topology import QueueTopology, declare, direct_exchange

__all__ = [
    "Action",
    "Confirmation",
    "ConsumerConfig",
    "DeadLetterFailed",
    "DeadLetterUnavailable",
    "DeadLetters",
    "ErrorCode",
    "Handler",
    "Policy",
    "QueueConsumer",
    "QueueTopology",
    "RetryBudgetExhausted",
    "confirmation_of",
    "create_broker",
    "declare",
    "direct_exchange",
    "is_temporary",
    "logger_levels",
]
