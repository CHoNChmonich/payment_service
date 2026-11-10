"""Интеграционные тесты RabbitMQ: шов публикатор → брокер → консьюмер → DLQ на реальном брокере.

Пропускаются, если не задан ``TEST_RABBITMQ_URL`` (окружение или ``.env``; на стенде это
брокер compose). Каждый тест работает в собственных обменниках и очередях с уникальным
суффиксом и удаляет их после себя: тесты не мешают ни друг другу, ни рабочим очередям стенда.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import uuid4

import aio_pika
import pytest
from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.adapters import new_payments, payment_events, processed_payments


class RabbitTestSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    TEST_RABBITMQ_URL: SecretStr


@dataclass(frozen=True)
class Names:
    """Имена объектов одного теста: общий обменник и DLX, по очереди и DLQ на консьюмер."""

    exchange: str
    queue: str
    routing_key: str
    dlx: str
    dlq: str
    processed_queue: str
    processed_routing_key: str
    processed_dlq: str


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    try:
        return RabbitTestSettings().TEST_RABBITMQ_URL.get_secret_value()
    except ValidationError:
        pytest.skip("TEST_RABBITMQ_URL не задан — интеграционные тесты RabbitMQ пропущены")


@pytest.fixture(scope="function")
async def names(rabbitmq_url: str) -> AsyncIterator[Names]:
    suffix = uuid4().hex[:8]
    created = Names(
        exchange=f"test.{suffix}.payments",
        queue=f"test.{suffix}.payments.new",
        routing_key="payments.new",
        dlx=f"test.{suffix}.payments.dlx",
        dlq=f"test.{suffix}.payments.new.dlq",
        processed_queue=f"test.{suffix}.payments.processed",
        processed_routing_key="payments.processed",
        processed_dlq=f"test.{suffix}.payments.processed.dlq",
    )

    yield created

    # Удаляем объекты теста, даже если тест их не успел объявить
    connection = await aio_pika.connect_robust(rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        for queue in (created.queue, created.dlq, created.processed_queue, created.processed_dlq):
            await channel.queue_delete(queue, if_unused=False, if_empty=False)
        for exchange in (created.exchange, created.dlx):
            await channel.exchange_delete(exchange, if_unused=False)


@pytest.fixture(scope="function")
def consumer_settings(rabbitmq_url: str, names: Names) -> new_payments.Settings:
    return new_payments.Settings.model_validate(
        {
            "RABBITMQ_URL": rabbitmq_url,
            "NEW_PAYMENTS_EXCHANGE": names.exchange,
            "NEW_PAYMENTS_QUEUE": names.queue,
            "NEW_PAYMENTS_ROUTING_KEY": names.routing_key,
            "NEW_PAYMENTS_DLX": names.dlx,
            "NEW_PAYMENTS_DLQ": names.dlq,
            "NEW_PAYMENTS_HANDLER_TIMEOUT": 5,
            "NEW_PAYMENTS_RETRY_BACKOFF": 0.05,
            "NEW_PAYMENTS_RETRY_BACKOFF_MAX": 0.1,
            "NEW_PAYMENTS_RETRY_BUDGET": 5,
            "NEW_PAYMENTS_DLQ_PUBLISH_TIMEOUT": 5,
            "NEW_PAYMENTS_PING_TIMEOUT": 2,
        }
    )


@pytest.fixture(scope="function")
def processed_settings(rabbitmq_url: str, names: Names) -> processed_payments.Settings:
    return processed_payments.Settings.model_validate(
        {
            "RABBITMQ_URL": rabbitmq_url,
            "PROCESSED_PAYMENTS_EXCHANGE": names.exchange,
            "PROCESSED_PAYMENTS_QUEUE": names.processed_queue,
            "PROCESSED_PAYMENTS_ROUTING_KEY": names.processed_routing_key,
            "PROCESSED_PAYMENTS_DLX": names.dlx,
            "PROCESSED_PAYMENTS_DLQ": names.processed_dlq,
            "PROCESSED_PAYMENTS_HANDLER_TIMEOUT": 5,
            "PROCESSED_PAYMENTS_RETRY_BACKOFF": 0.05,
            "PROCESSED_PAYMENTS_RETRY_BACKOFF_MAX": 0.1,
            "PROCESSED_PAYMENTS_RETRY_BUDGET": 5,
            "PROCESSED_PAYMENTS_DLQ_PUBLISH_TIMEOUT": 5,
            "PROCESSED_PAYMENTS_PING_TIMEOUT": 2,
        }
    )


@pytest.fixture(scope="function")
def publisher_settings(rabbitmq_url: str, names: Names) -> payment_events.Settings:
    return payment_events.Settings.model_validate(
        {
            "RABBITMQ_URL": rabbitmq_url,
            "PAYMENT_EVENTS_EXCHANGE": names.exchange,
            "PAYMENT_EVENTS_CREATED_ROUTING_KEY": names.routing_key,
            "PAYMENT_EVENTS_PROCESSED_ROUTING_KEY": names.processed_routing_key,
            "PAYMENT_EVENTS_PUBLISH_TIMEOUT": 5,
        }
    )
