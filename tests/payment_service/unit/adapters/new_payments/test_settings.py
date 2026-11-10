import pytest
from pydantic import SecretStr

from payment_service.adapters import new_payments, rabbitmq

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
URL = "amqp://guest:guest@rabbitmq:5672/"


def _settings(**overrides: object) -> new_payments.Settings:
    return new_payments.Settings.model_validate({"RABBITMQ_URL": URL} | overrides)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettings:
    def test__consumer_config(self) -> None:
        # Call
        config = new_payments.Settings(
            _env_file=None, RABBITMQ_URL=SecretStr(URL)
        ).consumer_config  # без .env

        # Assert: своя очередь и DLQ, общий обменник и DLX
        assert config.topology == rabbitmq.QueueTopology(
            exchange="payments",
            queue="payments.new",
            routing_key="payments.new",
            dead_letter_exchange="payments.dlx",
            dead_letter_queue="payments.new.dlq",
        )
        assert (
            config.handler_timeout
            == new_payments.Settings.model_fields["NEW_PAYMENTS_HANDLER_TIMEOUT"].default
        )

    def test__budget_below_first_pause_is_rejected(self) -> None:
        # Call and Assert: иначе первый же временный сбой остановил бы процесс
        with pytest.raises(ValueError, match="RETRY_BUDGET"):
            _settings(NEW_PAYMENTS_RETRY_BACKOFF=0.5, NEW_PAYMENTS_RETRY_BUDGET=0.1)
