"""Настройки доставки webhook и повторов."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.adapters import retry


class Settings(BaseSettings):
    """Доставка webhook: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        WEBHOOK_TIMEOUT: Общий дедлайн одной попытки доставки в секундах, от отправки запроса
            до прочтения ответа.
        WEBHOOK_RETRY_ATTEMPTS: Сколько всего попыток доставки, включая первую; после
            последней неудачи сообщение уходит в DLQ.
        WEBHOOK_RETRY_BACKOFF: Пауза перед второй попыткой в секундах; дальше удваивается.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    WEBHOOK_TIMEOUT: float = Field(default=5.0, gt=0, le=60)
    WEBHOOK_RETRY_ATTEMPTS: int = Field(default=3, ge=1, le=10)
    WEBHOOK_RETRY_BACKOFF: float = Field(default=1.0, ge=0, le=60)

    @property
    def retry_policy(self) -> retry.Retry:
        """Политика повторов доставки для ``RetryingWebhookNotifier``."""
        return retry.Retry(attempts=self.WEBHOOK_RETRY_ATTEMPTS, backoff=self.WEBHOOK_RETRY_BACKOFF)

    @property
    def worst_case(self) -> float:
        """Наибольшее время доставки в секундах: все попытки и паузы между ними.

        Столько транзакция ``notify`` держит строку платежа под блокировкой.
        """
        return self.WEBHOOK_RETRY_ATTEMPTS * self.WEBHOOK_TIMEOUT + self.retry_policy.total_backoff
