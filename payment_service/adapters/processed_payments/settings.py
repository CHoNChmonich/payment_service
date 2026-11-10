"""Настройки консьюмера проведённых платежей (уведомлений)."""

from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.adapters import rabbitmq


class Settings(BaseSettings):
    """Подключение, очереди и дедлайны консьюмера уведомлений: окружение и ``.env``.

    Attributes:
        RABBITMQ_URL: URL подключения ``amqp://user:password@host:port/vhost``; в логи не
            попадает.
        PROCESSED_PAYMENTS_EXCHANGE: Обменник событий платежей (direct), из которого читает
            очередь.
        PROCESSED_PAYMENTS_QUEUE: Очередь проведённых платежей; консьюмер её объявляет и
            владеет ею.
        PROCESSED_PAYMENTS_ROUTING_KEY: Ключ привязки очереди к обменнику; совпадает с
            ``PAYMENT_EVENTS_PROCESSED_ROUTING_KEY`` публикатора.
        PROCESSED_PAYMENTS_DLX: Обменник недоставленных сообщений (общий с ``new_payments``,
            объявление идемпотентно).
        PROCESSED_PAYMENTS_DLQ: Очередь недоставленных сообщений; она же ключ привязки к DLX.
        PROCESSED_PAYMENTS_HANDLER_TIMEOUT: Дедлайн одной попытки в секундах; превышение —
            временный сбой (повтор). Не меньше ожидания блокировки и худшего времени
            доставки webhook — связку проверяет composition root.
        PROCESSED_PAYMENTS_RETRY_BACKOFF: Пауза перед первым повтором при сбое инфраструктуры;
            дальше удваивается.
        PROCESSED_PAYMENTS_RETRY_BACKOFF_MAX: Наибольшая пауза между повторами.
        PROCESSED_PAYMENTS_RETRY_BUDGET: Сколько секунд одно сообщение может повторяться из-за
            сбоев инфраструктуры (и ждать DLQ); по исчерпании процесс останавливается без
            подтверждения. Не меньше ``PROCESSED_PAYMENTS_RETRY_BACKOFF``.
        PROCESSED_PAYMENTS_DLQ_PUBLISH_TIMEOUT: Дедлайн подтверждения записи в DLQ в секундах.
        PROCESSED_PAYMENTS_PING_TIMEOUT: Сколько секунд ждать соединения с брокером на каждой
            проверке цикла; без соединения метка «жив» не ставится.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    RABBITMQ_URL: SecretStr = Field(min_length=1)
    PROCESSED_PAYMENTS_EXCHANGE: str = Field(default="payments", min_length=1)
    PROCESSED_PAYMENTS_QUEUE: str = Field(default="payments.processed", min_length=1)
    PROCESSED_PAYMENTS_ROUTING_KEY: str = Field(default="payments.processed", min_length=1)
    PROCESSED_PAYMENTS_DLX: str = Field(default="payments.dlx", min_length=1)
    PROCESSED_PAYMENTS_DLQ: str = Field(default="payments.processed.dlq", min_length=1)
    PROCESSED_PAYMENTS_HANDLER_TIMEOUT: float = Field(default=60.0, gt=0, le=600)
    PROCESSED_PAYMENTS_RETRY_BACKOFF: float = Field(default=0.5, gt=0, le=60)
    PROCESSED_PAYMENTS_RETRY_BACKOFF_MAX: float = Field(default=10.0, gt=0, le=60)
    PROCESSED_PAYMENTS_RETRY_BUDGET: float = Field(default=120.0, gt=0, le=3600)
    PROCESSED_PAYMENTS_DLQ_PUBLISH_TIMEOUT: float = Field(default=5.0, gt=0, le=60)
    PROCESSED_PAYMENTS_PING_TIMEOUT: float = Field(default=5.0, gt=0, le=60)

    @model_validator(mode="after")
    def _budget_allows_a_retry(self) -> Self:
        """Бюджет повторов вмещает хотя бы первую паузу.

        Returns:
            Те же настройки.

        Raises:
            ValueError: ``PROCESSED_PAYMENTS_RETRY_BUDGET`` меньше
                ``PROCESSED_PAYMENTS_RETRY_BACKOFF``.
        """
        if self.PROCESSED_PAYMENTS_RETRY_BUDGET < self.PROCESSED_PAYMENTS_RETRY_BACKOFF:
            raise ValueError(
                "PROCESSED_PAYMENTS_RETRY_BUDGET не меньше PROCESSED_PAYMENTS_RETRY_BACKOFF"
            )
        return self

    @property
    def consumer_config(self) -> rabbitmq.ConsumerConfig:
        """Очередь, дедлайны и повторы для ``rabbitmq.QueueConsumer``."""
        return rabbitmq.ConsumerConfig(
            topology=rabbitmq.QueueTopology(
                exchange=self.PROCESSED_PAYMENTS_EXCHANGE,
                queue=self.PROCESSED_PAYMENTS_QUEUE,
                routing_key=self.PROCESSED_PAYMENTS_ROUTING_KEY,
                dead_letter_exchange=self.PROCESSED_PAYMENTS_DLX,
                dead_letter_queue=self.PROCESSED_PAYMENTS_DLQ,
            ),
            handler_timeout=self.PROCESSED_PAYMENTS_HANDLER_TIMEOUT,
            retry_backoff=self.PROCESSED_PAYMENTS_RETRY_BACKOFF,
            retry_backoff_max=self.PROCESSED_PAYMENTS_RETRY_BACKOFF_MAX,
            retry_budget=self.PROCESSED_PAYMENTS_RETRY_BUDGET,
            dlq_publish_timeout=self.PROCESSED_PAYMENTS_DLQ_PUBLISH_TIMEOUT,
            ping_timeout=self.PROCESSED_PAYMENTS_PING_TIMEOUT,
        )
