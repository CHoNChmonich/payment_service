"""Политики ошибок входящих адаптеров очередей: одна таблица у ``new_payments`` и
``processed_payments`` — один параметризованный тест на обе."""

from collections.abc import Callable
from types import ModuleType
from uuid import UUID

import pytest
from pydantic import ValidationError

from payment_service.adapters import new_payments, processed_payments, rabbitmq
from payment_service.application import dtos, errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")


class UncategorizedError(errors.Error):
    """Ошибка ядра без категории — дефект иерархии, а не плохое сообщение."""


def _validation_error() -> ValidationError:
    try:
        dtos.PaymentInfo.model_validate({})
    except ValidationError as exc:
        return exc
    raise AssertionError("ожидалась ошибка валидации")


ADAPTERS = pytest.mark.parametrize(
    "adapter", [new_payments, processed_payments], ids=["new_payments", "processed_payments"]
)

type ErrorFactory = Callable[[ModuleType], BaseException]


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
@ADAPTERS
class TestActionFor:
    @pytest.mark.parametrize(
        "make_error",
        [
            lambda adapter: adapter.InvalidMessage("не по контракту"),
            lambda _: errors.PaymentNotFound(id=PAYMENT_ID),
            lambda _: errors.IdempotencyKeyConflict(idempotency_key="k"),
            lambda _: errors.MessageConflict(message_id=PAYMENT_ID),
            lambda _: errors.PaymentTransitionNotAllowed(
                id=PAYMENT_ID, status="succeeded", action="process"
            ),
            lambda _: errors.WebhookRejected(url="u", status_code=404),
            lambda _: errors.WebhookAddressInvalid(url="u"),
            lambda _: errors.CredentialsInvalid(),
            lambda _: errors.WebhookUnavailable(url="u"),
            lambda _: errors.GatewayUnavailable(),
        ],
    )
    def test__dead_letter(self, adapter: ModuleType, make_error: ErrorFactory) -> None:
        # Call and Assert
        assert adapter.action_for(make_error(adapter)) is rabbitmq.Action.DEAD_LETTER

    @pytest.mark.parametrize(
        "error",
        [errors.StorageUnavailable(), errors.BrokerUnavailable(), TimeoutError()],
    )
    def test__retry(self, adapter: ModuleType, error: Exception) -> None:
        # Call and Assert
        assert adapter.action_for(error) is rabbitmq.Action.RETRY

    @pytest.mark.parametrize(
        "error",
        [UncategorizedError(), _validation_error(), RuntimeError("дефект"), KeyError("k")],
    )
    def test__stop(self, adapter: ModuleType, error: Exception) -> None:
        # Call and Assert
        assert adapter.action_for(error) is rabbitmq.Action.STOP

    def test__invalid_message_of_other_adapter_stops(self, adapter: ModuleType) -> None:
        # Setup: ошибка разбора чужого адаптера здесь не возникает — значит, дефект сборки
        other = processed_payments if adapter is new_payments else new_payments

        # Call and Assert
        assert adapter.action_for(other.InvalidMessage("x")) is rabbitmq.Action.STOP


@ADAPTERS
class TestErrorCode:
    def test__core_error(self, adapter: ModuleType) -> None:
        # Call and Assert
        assert adapter.error_code(errors.PaymentNotFound(id=PAYMENT_ID)) == "payment_not_found"

    def test__other_exception(self, adapter: ModuleType) -> None:
        # Call and Assert
        assert adapter.error_code(adapter.InvalidMessage("x")) == "InvalidMessage"
