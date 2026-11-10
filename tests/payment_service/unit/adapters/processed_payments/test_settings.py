import pytest
from pydantic import SecretStr

from payment_service.adapters import new_payments, payment_events, processed_payments, rabbitmq

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
URL = "amqp://guest:guest@rabbitmq:5672/"


def _settings(**overrides: object) -> processed_payments.Settings:
    return processed_payments.Settings.model_validate({"RABBITMQ_URL": URL} | overrides)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettings:
    def test__consumer_config(self) -> None:
        # Call
        config = processed_payments.Settings(
            _env_file=None, RABBITMQ_URL=SecretStr(URL)
        ).consumer_config  # без .env

        # Assert: своя очередь и DLQ, общий обменник и DLX
        assert config.topology == rabbitmq.QueueTopology(
            exchange="payments",
            queue="payments.processed",
            routing_key="payments.processed",
            dead_letter_exchange="payments.dlx",
            dead_letter_queue="payments.processed.dlq",
        )
        assert config.handler_timeout == 60

    def test__budget_below_first_pause_is_rejected(self) -> None:
        # Call and Assert: иначе первый же временный сбой остановил бы процесс
        with pytest.raises(ValueError, match="RETRY_BUDGET"):
            _settings(PROCESSED_PAYMENTS_RETRY_BACKOFF=0.5, PROCESSED_PAYMENTS_RETRY_BUDGET=0.1)


class TestRoutingKeys:
    def test__publisher_defaults_match_consumer_queues(self) -> None:
        # Setup: ключи публикатора и привязки консьюмеров — разные адаптеры, одно значение
        publisher = payment_events.Settings(_env_file=None, RABBITMQ_URL=SecretStr(URL))
        created = new_payments.Settings(_env_file=None, RABBITMQ_URL=SecretStr(URL))
        processed = processed_payments.Settings(_env_file=None, RABBITMQ_URL=SecretStr(URL))

        # Assert
        assert publisher.PAYMENT_EVENTS_CREATED_ROUTING_KEY == created.NEW_PAYMENTS_ROUTING_KEY
        assert (
            publisher.PAYMENT_EVENTS_PROCESSED_ROUTING_KEY
            == processed.PROCESSED_PAYMENTS_ROUTING_KEY
        )
        assert publisher.PAYMENT_EVENTS_EXCHANGE == created.NEW_PAYMENTS_EXCHANGE
        assert publisher.PAYMENT_EVENTS_EXCHANGE == processed.PROCESSED_PAYMENTS_EXCHANGE
