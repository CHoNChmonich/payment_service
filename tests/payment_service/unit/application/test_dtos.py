from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from payment_service.application import dtos, entities

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
VALID: dict[str, Any] = {
    "idempotency_key": "key-1",
    "amount": Decimal("10.00"),
    "currency": "RUB",
    "webhook_url": "http://webhook_sink:8080/hook",
}


def _new_payment_info(**overrides: object) -> dtos.NewPaymentInfo:
    return dtos.NewPaymentInfo(**{**VALID, **overrides})


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestNewPaymentInfo:
    def test__defaults(self) -> None:
        # Call
        info = _new_payment_info()

        # Assert
        assert info.description == ""
        assert info.metadata == {}
        assert info.currency is entities.Currency.RUB
        assert str(info.webhook_url) == "http://webhook_sink:8080/hook"

    @pytest.mark.parametrize("amount", ["10", "10.5", "0.01", "9999999999999999.99"])
    def test__amount_accepted(self, amount: str) -> None:
        # Call
        info = _new_payment_info(amount=Decimal(amount))

        # Assert
        assert info.amount == Decimal(amount)

    @pytest.mark.parametrize(
        "amount",
        ["0", "-1", "0.001", "10.001", "99999999999999999.99", "NaN", "Infinity"],
    )
    def test__bad_amount(self, amount: str) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="amount"):
            _new_payment_info(amount=Decimal(amount))

    def test__bad_currency(self) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="currency"):
            _new_payment_info(currency="GBP")

    @pytest.mark.parametrize("key", ["", " ", " key", "key ", "a" * 256, "key\nkey"])
    def test__bad_idempotency_key(self, key: str) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="idempotency_key"):
            _new_payment_info(idempotency_key=key)

    def test__idempotency_key_at_limit(self) -> None:
        # Call
        info = _new_payment_info(idempotency_key="a" * dtos.MAX_IDEMPOTENCY_KEY_LENGTH)

        # Assert
        assert len(info.idempotency_key) == dtos.MAX_IDEMPOTENCY_KEY_LENGTH

    def test__description_too_long(self) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="description"):
            _new_payment_info(description="x" * (dtos.MAX_DESCRIPTION_LENGTH + 1))

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://example.com/hook",
            "example.com/hook",
            "http://user:secret@example.com/hook",
            "http://user@example.com/hook",
            "http://",
            "http://example.com/" + "a" * dtos.MAX_WEBHOOK_URL_LENGTH,
        ],
    )
    def test__bad_webhook_url(self, url: str) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="webhook_url"):
            _new_payment_info(webhook_url=url)

    def test__webhook_url_normalized(self) -> None:
        # Call
        info = _new_payment_info(webhook_url="HTTP://Example.COM:80")

        # Assert
        assert str(info.webhook_url) == "http://example.com/"

    def test__metadata_nested(self) -> None:
        # Call
        info = _new_payment_info(metadata={"order": 1, "tags": ["a", None], "x": {"y": 1.5}})

        # Assert
        assert info.metadata == {"order": 1, "tags": ["a", None], "x": {"y": 1.5}}

    @pytest.mark.parametrize(
        ("metadata", "message"),
        [
            ({"a": float("inf")}, "недопустимые в JSON"),
            ({"a": float("nan")}, "недопустимые в JSON"),
            ({"a": "x\x00y"}, "u0000"),
            ({"a": "x" * dtos.MAX_METADATA_BYTES}, "длиннее"),
            ("not a dict", "metadata"),
            ({1: "a"}, "metadata"),
        ],
    )
    def test__bad_metadata(self, metadata: object, message: str) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match=message):
            _new_payment_info(metadata=metadata)


class TestPaymentInfo:
    def test__from_entity(self) -> None:
        # Setup
        payment = entities.Payment(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            idempotency_key="key-1",
            amount=Decimal("10.00"),
            currency=entities.Currency.USD,
            description="Заказ",
            metadata={"order": 1},
            webhook_url="http://webhook_sink:8080/hook",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        # Call
        info = dtos.PaymentInfo.model_validate(payment)

        # Assert
        assert info.model_dump() == {
            "id": UUID("00000000-0000-0000-0000-000000000001"),
            "idempotency_key": "key-1",
            "amount": Decimal("10.00"),
            "currency": entities.Currency.USD,
            "description": "Заказ",
            "metadata": {"order": 1},
            "webhook_url": "http://webhook_sink:8080/hook",
            "status": entities.PaymentStatus.PENDING,
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "processed_at": None,
            "notified_at": None,
        }


class TestPaymentCreated:
    def test__defaults(self) -> None:
        # Call
        event = dtos.PaymentCreated(payment_id=UUID("00000000-0000-0000-0000-000000000001"))

        # Assert
        assert event.type == "payment_created"
        assert event.occurred_at.tzinfo is not None
        assert event.event_id != dtos.PaymentCreated(payment_id=event.payment_id).event_id
