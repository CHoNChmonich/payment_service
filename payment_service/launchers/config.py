"""Настройки уровня приложения, которые не принадлежат ни одному адаптеру."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE


class Settings(BaseSettings):
    """Настройки уровня приложения: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        RELAY_BATCH_SIZE: Наибольшее число событий outbox, публикуемых одной транзакцией;
            ограничивает и время, на которое транзакция публикатора остаётся открытой.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    RELAY_BATCH_SIZE: int = Field(default=100, ge=1, le=10_000)
