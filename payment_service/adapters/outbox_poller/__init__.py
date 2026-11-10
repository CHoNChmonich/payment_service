"""Входящий адаптер по таймеру: опрос outbox вызывает сценарий ``interfaces.EventRelay``."""

from .poller import OutboxPoller
from .settings import Settings

__all__ = ["OutboxPoller", "Settings"]
