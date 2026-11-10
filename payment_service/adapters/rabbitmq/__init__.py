"""Общее для адаптеров RabbitMQ: фабрика брокера, описание обменника, классификация сбоев.

Библиотека, а не адаптер: портов не реализует, адаптеры и ядро не импортирует. Её используют
``payment_events`` (выход: публикация событий) и ``new_payments`` (вход: консьюмер и запись в
DLQ), чтобы настройки канала, правило «временный сбой или постоянный» и трактовка подтверждения
публикации жили в одном месте; друг о друге эти адаптеры не знают.
"""

from .broker import create_broker, logger_levels
from .errors import Confirmation, confirmation_of, is_temporary
from .topology import direct_exchange

__all__ = [
    "Confirmation",
    "confirmation_of",
    "create_broker",
    "direct_exchange",
    "is_temporary",
    "logger_levels",
]
