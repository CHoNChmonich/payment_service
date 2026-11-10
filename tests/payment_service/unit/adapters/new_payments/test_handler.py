import json
from unittest.mock import Mock
from uuid import UUID

import pytest

from payment_service.adapters import new_payments
from payment_service.application import dtos, errors, interfaces

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
EVENT = dtos.PaymentEventInfo(
    event_id=UUID("11111111-1111-1111-1111-111111111111"),
    payment_id=UUID("22222222-2222-2222-2222-222222222222"),
)
BODY = json.dumps(
    {
        "event_id": str(EVENT.event_id),
        "type": "payment_created",
        "payment_id": str(EVENT.payment_id),
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
    @pytest.mark.parametrize("applied", [True, False])
    async def test__processes(
        self, handler: new_payments.NewPaymentsHandler, payment: Mock, applied: bool
    ) -> None:
        # Setup
        payment.process.return_value = applied

        # Call
        result = await handler.handle(BODY)

        # Assert: только проведение — уведомление идёт отдельным событием
        assert result is applied
        payment.process.assert_awaited_once_with(EVENT)
        payment.notify.assert_not_called()

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
        payment.process.side_effect = errors.PaymentNotFound(id=EVENT.payment_id)

        # Call and Assert
        with pytest.raises(errors.PaymentNotFound):
            await handler.handle(BODY)
