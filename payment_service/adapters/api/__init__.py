"""Входящий HTTP-адаптер (FastAPI).

Обработчики — методы класса ``Controller``; порт сценариев (``interfaces.Payment``) и
аутентификатор (``interfaces.Authenticator``) приходят в его конструктор из composition root тем
же «чистым» DI, что и у остальных адаптеров, FastAPI-``Depends`` не используется.
Маршруты объявляются императивно в ``create_router``, приложение собирает ``create_app``.
"""

from .app import create_app
from .lifespan import create_lifespan
from .v1 import Controller as V1Controller

__all__ = ["V1Controller", "create_app", "create_lifespan"]
