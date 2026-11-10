"""Настройки опроса outbox."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE


class Settings(BaseSettings):
    """Ритм опроса outbox: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        OUTBOX_POLLER_IDLE_INTERVAL: Пауза в секундах после пустого вызова доставки (или сбоя
            зависимости); пока публикуется, следующий вызов идёт сразу.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    OUTBOX_POLLER_IDLE_INTERVAL: float = Field(default=1.0, gt=0, le=60)
