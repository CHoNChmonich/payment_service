from uuid import UUID

import pytest
from pydantic import ValidationError

from payment_service.adapters import new_payments
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


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestActionFor:
    @pytest.mark.parametrize(
        "error",
        [
            new_payments.InvalidMessage("не по контракту"),
            errors.PaymentNotFound(id=PAYMENT_ID),
            errors.IdempotencyKeyConflict(idempotency_key="k"),
            errors.PaymentNotProcessed(id=PAYMENT_ID),
            errors.WebhookRejected(url="u", status_code=404),
            errors.CredentialsInvalid(),
            errors.WebhookUnavailable(url="u"),
            errors.GatewayUnavailable(),
        ],
    )
    def test__dead_letter(self, error: Exception) -> None:
        # Call and Assert
        assert new_payments.action_for(error) is new_payments.Action.DEAD_LETTER

    @pytest.mark.parametrize(
        "error",
        [errors.StorageUnavailable(), errors.BrokerUnavailable(), TimeoutError()],
    )
    def test__retry(self, error: Exception) -> None:
        # Call and Assert
        assert new_payments.action_for(error) is new_payments.Action.RETRY

    @pytest.mark.parametrize(
        "error",
        [UncategorizedError(), _validation_error(), RuntimeError("дефект"), KeyError("k")],
    )
    def test__stop(self, error: Exception) -> None:
        # Call and Assert
        assert new_payments.action_for(error) is new_payments.Action.STOP
