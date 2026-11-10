import json
from datetime import UTC, datetime
from uuid import UUID

from payment_service.adapters import payment_events
from payment_service.application import dtos

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
EVENT = dtos.PaymentCreated(
    event_id=UUID("11111111-1111-1111-1111-111111111111"),
    occurred_at=datetime(2026, 11, 10, 12, 0, 0, 500000, tzinfo=UTC),
    payment_id=UUID("22222222-2222-2222-2222-222222222222"),
)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPaymentCreatedMessage:
    def test__from_application_and_body(self) -> None:
        # Call
        message = payment_events.PaymentCreatedMessage.from_application(EVENT)

        # Assert: время — ISO 8601 с зоной, UUID — строками
        assert json.loads(message.body()) == {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "type": "payment_created",
            "occurred_at": "2026-11-10T12:00:00.500000Z",
            "payment_id": "22222222-2222-2222-2222-222222222222",
        }
        assert message.headers() == {"x-event-type": "payment_created"}
