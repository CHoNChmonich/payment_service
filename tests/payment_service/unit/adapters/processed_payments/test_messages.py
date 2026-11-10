"""Шов публикатор ↔ консьюмер уведомлений: тело ``payment_events`` принимает
``processed_payments``."""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from payment_service.adapters import payment_events, processed_payments
from payment_service.application import dtos

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
EVENT = dtos.PaymentProcessed(
    event_id=UUID("11111111-1111-1111-1111-111111111111"),
    occurred_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    payment_id=UUID("22222222-2222-2222-2222-222222222222"),
)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPaymentProcessedMessage:
    def test__accepts_published_body(self) -> None:
        # Setup
        body = payment_events.PaymentEventMessage.from_application(EVENT).body()

        # Call
        message = processed_payments.PaymentProcessedMessage.model_validate_json(body)

        # Assert
        assert message.to_application() == dtos.PaymentEventInfo(
            event_id=EVENT.event_id, payment_id=EVENT.payment_id
        )
        assert message.occurred_at == EVENT.occurred_at

    def test__ignores_unknown_fields(self) -> None:
        # Setup
        body = json.dumps(
            {
                "event_id": str(EVENT.event_id),
                "type": "payment_processed",
                "payment_id": str(EVENT.payment_id),
                "extra": 1,
            }
        )

        # Call
        message = processed_payments.PaymentProcessedMessage.model_validate_json(body)

        # Assert
        assert message.occurred_at is None
        assert message.to_application().payment_id == EVENT.payment_id

    @pytest.mark.parametrize(
        "body",
        [
            b"not json",
            b"{}",
            json.dumps({"event_id": "x", "type": "payment_processed", "payment_id": "y"}),
            payment_events.PaymentEventMessage.from_application(
                dtos.PaymentCreated(payment_id=EVENT.payment_id)
            ).body(),
        ],
        ids=["not_json", "empty", "bad_uuids", "created_event"],
    )
    def test__rejects_bad_bodies(self, body: bytes | str) -> None:
        # Call and Assert
        with pytest.raises(ValidationError):
            processed_payments.PaymentProcessedMessage.model_validate_json(body)
