"""Настройки логирования."""

from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE

type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    """Логирование: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        LOG_LEVEL: Уровень корневого логгера; регистр не важен, неизвестный уровень — ошибка
            конфигурации на старте.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    LOG_LEVEL: LogLevel = "INFO"

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _uppercase_level(cls, value: object) -> object:
        """Приводит уровень к верхнему регистру: ``info`` и ``INFO`` равнозначны.

        Args:
            value: Сырое значение из окружения.

        Returns:
            Значение в верхнем регистре, если это строка; иначе как есть.
        """
        return value.upper() if isinstance(value, str) else value
