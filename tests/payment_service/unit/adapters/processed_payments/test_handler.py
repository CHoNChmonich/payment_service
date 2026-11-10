import json
from unittest.mock import Mock
from uuid import UUID

import pytest

from payment_service.adapters import processed_payments
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
        "type": "payment_processed",
        "payment_id": str(EVENT.payment_id),
    }
).encode()


@pytest.fixture(scope="function")
def payment() -> Mock:
    return Mock(interfaces.Payment)


@pytest.fixture(scope="function")
def handler(payment: Mock) -> processed_payments.ProcessedPaymentsHandler:
    return processed_payments.ProcessedPaymentsHandler(payment)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestHandle:
    @pytest.mark.parametrize("applied", [True, False])
    async def test__notifies(
        self, handler: processed_payments.ProcessedPaymentsHandler, payment: Mock, applied: bool
    ) -> None:
        # Setup
        payment.notify.return_value = applied

        # Call
        result = await handler.handle(BODY)

        # Assert
        assert result is applied
        payment.notify.assert_awaited_once_with(EVENT)
        payment.process.assert_not_called()

    @pytest.mark.parametrize(
        "body",
        [b'{"type": "payment_processed"}', BODY.replace(b"payment_processed", b"payment_created")],
        ids=["incomplete", "created_event"],
    )
    async def test__bad_body_is_invalid_message(
        self, handler: processed_payments.ProcessedPaymentsHandler, payment: Mock, body: bytes
    ) -> None:
        # Call and Assert
        with pytest.raises(processed_payments.InvalidMessage):
            await handler.handle(body)
        payment.notify.assert_not_called()

    async def test__core_errors_pass_through(
        self, handler: processed_payments.ProcessedPaymentsHandler, payment: Mock
    ) -> None:
        # Setup
        payment.notify.side_effect = errors.WebhookRejected(url="u", status_code=410)

        # Call and Assert
        with pytest.raises(errors.WebhookRejected):
            await handler.handle(BODY)
