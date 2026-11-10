import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from payment_service.adapters import payment_events
from payment_service.application import dtos

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
EVENT_ID = UUID("11111111-1111-1111-1111-111111111111")
OCCURRED_AT = datetime(2026, 11, 10, 12, 0, 0, 500000, tzinfo=UTC)
PAYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPaymentEventMessage:
    @pytest.mark.parametrize(
        "event",
        [
            dtos.PaymentCreated(event_id=EVENT_ID, occurred_at=OCCURRED_AT, payment_id=PAYMENT_ID),
            dtos.PaymentProcessed(
                event_id=EVENT_ID, occurred_at=OCCURRED_AT, payment_id=PAYMENT_ID
            ),
        ],
        ids=["payment_created", "payment_processed"],
    )
    def test__from_application_and_body(self, event: dtos.AnyPaymentEvent) -> None:
        # Call
        message = payment_events.PaymentEventMessage.from_application(event)

        # Assert: время — ISO 8601 с зоной, UUID — строками, тип — в теле и в заголовке
        assert json.loads(message.body()) == {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "type": event.type,
            "occurred_at": "2026-11-10T12:00:00.500000Z",
            "payment_id": "22222222-2222-2222-2222-222222222222",
        }
        assert message.headers() == {"x-event-type": event.type}
