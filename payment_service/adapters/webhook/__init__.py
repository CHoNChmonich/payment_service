"""Исходящий адаптер webhook: доставка уведомления о результате платежа по HTTP (httpx).

Реализует порт ``interfaces.WebhookNotifier``. ``messages`` — тело уведомления (контракт с
получателем), ``notifier`` — отправка и трактовка ответа, ``retrying`` — Decorator с повторами
на библиотеке ``retry``; собирает их composition root каждого процесса, которому нужен сервис
платежа.
"""

from .messages import CONTENT_TYPE, PAYMENT_ID_HEADER, WebhookMessage
from .notifier import WebhookNotifier, logger_levels
from .retrying import RetryingWebhookNotifier
from .settings import Settings

__all__ = [
    "CONTENT_TYPE",
    "PAYMENT_ID_HEADER",
    "RetryingWebhookNotifier",
    "Settings",
    "WebhookMessage",
    "WebhookNotifier",
    "logger_levels",
]
