"""Настройки статической аутентификации по API-ключу."""

import re

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.application import dtos

# Короткий ключ подбирается перебором; граница — как у типичных секретов API.
MIN_API_KEY_LENGTH = 16
# Синтаксис ключа — общий с HTTP-адаптером (``dtos.base``): иное адаптер отбрасывает как мусор до
# сверки, и такой ключ никогда не совпал бы (401 на каждый запрос).
_API_KEY = re.compile(dtos.API_KEY_PATTERN)


class Settings(BaseSettings):
    """API-ключ сервиса: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        AUTH_API_KEY: Единственный действительный ключ; клиенты передают его в ``X-API-Key``.
            В логи, ``repr`` и текст ошибок валидации не попадает. От ``MIN_API_KEY_LENGTH`` до
            ``dtos.MAX_API_KEY_LENGTH`` символов, только видимые ASCII без пробелов.
    """

    model_config = SettingsConfigDict(
        env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True
    )

    AUTH_API_KEY: SecretStr = Field(
        min_length=MIN_API_KEY_LENGTH, max_length=dtos.MAX_API_KEY_LENGTH
    )

    @field_validator("AUTH_API_KEY")
    @classmethod
    def _printable_ascii(cls, value: SecretStr) -> SecretStr:
        """Отклоняет ключ, который не пройдёт разбор заголовка.

        Args:
            value: Ключ из окружения.

        Returns:
            Тот же ключ.

        Raises:
            ValueError: В ключе есть пробелы или символы вне видимого ASCII.
        """
        if not _API_KEY.fullmatch(value.get_secret_value()):
            raise ValueError("AUTH_API_KEY — только видимые символы ASCII без пробелов")
        return value

    @property
    def api_key(self) -> str:
        """Значение ключа для сверки.

        Returns:
            Ключ как строка.
        """
        return self.AUTH_API_KEY.get_secret_value()
