"""Порты ядра: входящие (сценарии) и исходящие (репозитории, Unit of Work, внешние системы).

Входящие порты — ``Payment``, ``EventRelay``: их реализуют ``services.X``, от них зависят
входящие адаптеры (HTTP-контроллер, консьюмер, опрос outbox). Исходящие — ``PaymentsRepo``,
``OutboxRepo``, ``InboxRepo``, ``UnitOfWork``: их реализует ``adapters/database``;
``EventPublisher`` — ``adapters/payment_events``; ``PaymentGateway`` — ``adapters/payment_gateway``;
``WebhookNotifier`` — ``adapters/webhook``; все они потребляются сервисами. ``Authenticator`` —
исходящий порт, который потребляет входящий адаптер (контроллер), а не сервис: сценария нет,
поэтому нет и сервиса; реализует ``adapters/auth``. Реализации носят имена портов и различаются
модулем импорта (``interfaces.Payment`` / ``services.Payment``, ``interfaces.PaymentsRepo`` /
``database.repositories.PaymentsRepo``).
"""

from .authenticator import Authenticator
from .event_publisher import EventPublisher
from .event_relay import EventRelay
from .inbox import InboxRepo
from .outbox import OutboxRepo
from .payment import Payment
from .payment_gateway import PaymentGateway
from .payments import PaymentsRepo
from .unit_of_work import UnitOfWork, UnitOfWorkFactory
from .webhook_notifier import WebhookNotifier

__all__ = [
    "Authenticator",
    "EventPublisher",
    "EventRelay",
    "InboxRepo",
    "OutboxRepo",
    "Payment",
    "PaymentGateway",
    "PaymentsRepo",
    "UnitOfWork",
    "UnitOfWorkFactory",
    "WebhookNotifier",
]
