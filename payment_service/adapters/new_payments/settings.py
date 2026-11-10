"""Настройки консьюмера новых платежей."""

from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.adapters import rabbitmq


class Settings(BaseSettings):
    """Подключение, очереди и дедлайны консьюмера: переменные окружения и ``.env``.

    Attributes:
        RABBITMQ_URL: URL подключения ``amqp://user:password@host:port/vhost``; в логи не
            попадает.
        NEW_PAYMENTS_EXCHANGE: Обменник событий платежей (direct), из которого читает очередь.
        NEW_PAYMENTS_QUEUE: Очередь новых платежей; консьюмер её объявляет и владеет ею.
        NEW_PAYMENTS_ROUTING_KEY: Ключ привязки очереди к обменнику.
        NEW_PAYMENTS_DLX: Обменник недоставленных сообщений.
        NEW_PAYMENTS_DLQ: Очередь недоставленных сообщений; она же ключ привязки к DLX.
        NEW_PAYMENTS_HANDLER_TIMEOUT: Дедлайн одной попытки обработки сообщения в секундах;
            превышение — временный сбой (повтор). Не меньше ожидания блокировки и худшего
            времени шлюза — связку проверяет composition root.
        NEW_PAYMENTS_RETRY_BACKOFF: Пауза перед первым повтором при сбое инфраструктуры;
            дальше удваивается.
        NEW_PAYMENTS_RETRY_BACKOFF_MAX: Наибольшая пауза между повторами.
        NEW_PAYMENTS_RETRY_BUDGET: Сколько секунд одно сообщение может повторяться из-за
            сбоев инфраструктуры (и ждать DLQ); по исчерпании процесс останавливается без
            подтверждения сообщения. Не меньше ``NEW_PAYMENTS_RETRY_BACKOFF``: меньший бюджет
            означал бы остановку на первом же временном сбое.
        NEW_PAYMENTS_DLQ_PUBLISH_TIMEOUT: Дедлайн подтверждения записи в DLQ в секундах.
        NEW_PAYMENTS_PING_TIMEOUT: Сколько секунд ждать соединения с брокером на каждой
            проверке цикла; без соединения метка «жив» не ставится.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    RABBITMQ_URL: SecretStr = Field(min_length=1)
    NEW_PAYMENTS_EXCHANGE: str = Field(default="payments", min_length=1)
    NEW_PAYMENTS_QUEUE: str = Field(default="payments.new", min_length=1)
    NEW_PAYMENTS_ROUTING_KEY: str = Field(default="payments.new", min_length=1)
    NEW_PAYMENTS_DLX: str = Field(default="payments.dlx", min_length=1)
    NEW_PAYMENTS_DLQ: str = Field(default="payments.new.dlq", min_length=1)
    NEW_PAYMENTS_HANDLER_TIMEOUT: float = Field(default=60.0, gt=0, le=600)
    NEW_PAYMENTS_RETRY_BACKOFF: float = Field(default=0.5, gt=0, le=60)
    NEW_PAYMENTS_RETRY_BACKOFF_MAX: float = Field(default=10.0, gt=0, le=60)
    NEW_PAYMENTS_RETRY_BUDGET: float = Field(default=120.0, gt=0, le=3600)
    NEW_PAYMENTS_DLQ_PUBLISH_TIMEOUT: float = Field(default=5.0, gt=0, le=60)
    NEW_PAYMENTS_PING_TIMEOUT: float = Field(default=5.0, gt=0, le=60)

    @model_validator(mode="after")
    def _budget_allows_a_retry(self) -> Self:
        """Бюджет повторов вмещает хотя бы первую паузу.

        Returns:
            Те же настройки.

        Raises:
            ValueError: ``NEW_PAYMENTS_RETRY_BUDGET`` меньше ``NEW_PAYMENTS_RETRY_BACKOFF``.
        """
        if self.NEW_PAYMENTS_RETRY_BUDGET < self.NEW_PAYMENTS_RETRY_BACKOFF:
            raise ValueError("NEW_PAYMENTS_RETRY_BUDGET не меньше NEW_PAYMENTS_RETRY_BACKOFF")
        return self

    @property
    def consumer_config(self) -> rabbitmq.ConsumerConfig:
        """Очередь, дедлайны и повторы для ``rabbitmq.QueueConsumer``."""
        return rabbitmq.ConsumerConfig(
            topology=rabbitmq.QueueTopology(
                exchange=self.NEW_PAYMENTS_EXCHANGE,
                queue=self.NEW_PAYMENTS_QUEUE,
                routing_key=self.NEW_PAYMENTS_ROUTING_KEY,
                dead_letter_exchange=self.NEW_PAYMENTS_DLX,
                dead_letter_queue=self.NEW_PAYMENTS_DLQ,
            ),
            handler_timeout=self.NEW_PAYMENTS_HANDLER_TIMEOUT,
            retry_backoff=self.NEW_PAYMENTS_RETRY_BACKOFF,
            retry_backoff_max=self.NEW_PAYMENTS_RETRY_BACKOFF_MAX,
            retry_budget=self.NEW_PAYMENTS_RETRY_BUDGET,
            dlq_publish_timeout=self.NEW_PAYMENTS_DLQ_PUBLISH_TIMEOUT,
            ping_timeout=self.NEW_PAYMENTS_PING_TIMEOUT,
        )
