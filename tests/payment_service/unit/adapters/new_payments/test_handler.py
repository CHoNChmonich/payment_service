import json
from unittest.mock import Mock, call
from uuid import UUID

import pytest

from payment_service.adapters import new_payments
from payment_service.application import errors, interfaces

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")
BODY = json.dumps(
    {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "type": "payment_created",
        "payment_id": str(PAYMENT_ID),
    }
).encode()


@pytest.fixture(scope="function")
def payment() -> Mock:
    return Mock(interfaces.Payment)


@pytest.fixture(scope="function")
def handler(payment: Mock) -> new_payments.NewPaymentsHandler:
    return new_payments.NewPaymentsHandler(payment)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestHandle:
    async def test__processes_then_notifies(
        self, handler: new_payments.NewPaymentsHandler, payment: Mock
    ) -> None:
        # Setup
        payment.notify.return_value = True

        # Call
        notified = await handler.handle(BODY)

        # Assert: webhook — только после проведения платежа
        assert notified is True
        assert payment.mock_calls == [call.process(PAYMENT_ID), call.notify(PAYMENT_ID)]

    async def test__bad_body_is_invalid_message(
        self, handler: new_payments.NewPaymentsHandler, payment: Mock
    ) -> None:
        # Call and Assert
        with pytest.raises(new_payments.InvalidMessage):
            await handler.handle(b'{"type": "payment_created"}')
        payment.process.assert_not_called()

    async def test__core_errors_pass_through(
        self, handler: new_payments.NewPaymentsHandler, payment: Mock
    ) -> None:
        # Setup
        payment.process.side_effect = errors.PaymentNotFound(id=PAYMENT_ID)

        # Call and Assert
        with pytest.raises(errors.PaymentNotFound):
            await handler.handle(BODY)
        payment.notify.assert_not_called()
