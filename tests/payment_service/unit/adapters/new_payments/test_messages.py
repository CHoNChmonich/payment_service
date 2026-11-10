"""Шов публикатор ↔ консьюмер: тело, которое публикует ``payment_events``, принимает консьюмер."""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from payment_service.adapters import new_payments, payment_events
from payment_service.application import dtos

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
EVENT = dtos.PaymentCreated(
    event_id=UUID("11111111-1111-1111-1111-111111111111"),
    occurred_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    payment_id=UUID("22222222-2222-2222-2222-222222222222"),
)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPaymentCreatedMessage:
    def test__accepts_published_body(self) -> None:
        # Setup
        body = payment_events.PaymentCreatedMessage.from_application(EVENT).body()

        # Call
        message = new_payments.PaymentCreatedMessage.model_validate_json(body)

        # Assert
        assert message.to_application() == EVENT.payment_id
        assert message.event_id == EVENT.event_id

    def test__ignores_unknown_fields(self) -> None:
        # Setup
        body = json.dumps(
            {
                "event_id": str(EVENT.event_id),
                "type": "payment_created",
                "payment_id": str(EVENT.payment_id),
                "extra": 1,
            }
        )

        # Call
        message = new_payments.PaymentCreatedMessage.model_validate_json(body)

        # Assert
        assert message.occurred_at is None
        assert message.to_application() == EVENT.payment_id

    @pytest.mark.parametrize(
        "body",
        [
            b"not json",
            b"{}",
            json.dumps({"event_id": "x", "type": "payment_created", "payment_id": "y"}),
            json.dumps(
                {
                    "event_id": str(EVENT.event_id),
                    "type": "payment_refunded",
                    "payment_id": str(EVENT.payment_id),
                }
            ),
        ],
    )
    def test__rejects_bad_bodies(self, body: bytes | str) -> None:
        # Call and Assert
        with pytest.raises(ValidationError):
            new_payments.PaymentCreatedMessage.model_validate_json(body)
