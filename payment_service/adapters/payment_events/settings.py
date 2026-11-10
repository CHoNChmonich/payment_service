"""Настройки публикации событий платежей в RabbitMQ."""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE


class Settings(BaseSettings):
    """Подключение к RabbitMQ и обменник событий: переменные окружения и ``.env``.

    Attributes:
        RABBITMQ_URL: URL подключения ``amqp://user:password@host:port/vhost``; в логи не
            попадает.
        PAYMENT_EVENTS_EXCHANGE: Обменник событий платежей (direct); публикатор его объявляет.
        PAYMENT_EVENTS_ROUTING_KEY: Ключ маршрутизации события «платёж создан» — имя очереди
            консьюмера.
        PAYMENT_EVENTS_PUBLISH_TIMEOUT: Дедлайн подтверждения одной публикации в секундах;
            превышение — временный сбой брокера.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    RABBITMQ_URL: SecretStr = Field(min_length=1)
    PAYMENT_EVENTS_EXCHANGE: str = Field(default="payments", min_length=1)
    PAYMENT_EVENTS_ROUTING_KEY: str = Field(default="payments.new", min_length=1)
    PAYMENT_EVENTS_PUBLISH_TIMEOUT: float = Field(default=5.0, gt=0, le=60)

    @property
    def stop_grace(self) -> float:
        """Сколько секунд нужно на остановку relay: текущая публикация и закрытие брокера.

        ``stop_grace_period`` контейнера — не меньше.
        """
        return 2 * self.PAYMENT_EVENTS_PUBLISH_TIMEOUT + 5
