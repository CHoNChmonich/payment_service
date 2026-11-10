"""Исходящий адаптер аутентификации: статический API-ключ, реализация ``interfaces.Authenticator``.

Потребитель порта — HTTP-адаптер; друг о друге адаптеры не знают, связывает их composition
root. Внешнего сервиса нет, поэтому у адаптера нет соединений и хуков жизненного цикла.
"""

from .authenticator import Authenticator
from .settings import MAX_API_KEY_LENGTH, MIN_API_KEY_LENGTH, Settings

__all__ = ["MAX_API_KEY_LENGTH", "MIN_API_KEY_LENGTH", "Authenticator", "Settings"]
