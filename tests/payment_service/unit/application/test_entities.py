from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from payment_service.application import entities, errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT_ID = UUID("00000000-0000-0000-0000-000000000001")
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)


def _payment(
    status: entities.PaymentStatus = entities.PaymentStatus.PENDING,
    *,
    processed_at: datetime | None = None,
) -> entities.Payment:
    return entities.Payment(
        id=PAYMENT_ID,
        idempotency_key="key-1",
        amount=Decimal("10.00"),
        currency=entities.Currency.RUB,
        description="Заказ 1",
        metadata={"order": 1, "tags": ["a", "b"]},
        webhook_url="http://webhook_sink:8080/hook",
        status=status,
        processed_at=processed_at,
    )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSucceed:
    def test__succeed(self) -> None:
        # Setup
        payment = _payment()

        # Call
        payment.succeed(NOW)

        # Assert
        assert payment.status is entities.PaymentStatus.SUCCEEDED
        assert payment.processed_at == NOW
        assert payment.is_final

    @pytest.mark.parametrize(
        "status", [entities.PaymentStatus.SUCCEEDED, entities.PaymentStatus.FAILED]
    )
    def test__already_processed(self, status: entities.PaymentStatus) -> None:
        # Setup
        payment = _payment(status, processed_at=NOW)

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed) as exc_info:
            payment.succeed(LATER)

        assert exc_info.value.context == {"id": PAYMENT_ID, "status": status, "action": "succeed"}
        assert payment.status is status
        assert payment.processed_at == NOW


class TestFail:
    def test__fail(self) -> None:
        # Setup
        payment = _payment()

        # Call
        payment.fail(NOW)

        # Assert
        assert payment.status is entities.PaymentStatus.FAILED
        assert payment.processed_at == NOW
        assert payment.is_final

    @pytest.mark.parametrize(
        "status", [entities.PaymentStatus.SUCCEEDED, entities.PaymentStatus.FAILED]
    )
    def test__already_processed(self, status: entities.PaymentStatus) -> None:
        # Setup
        payment = _payment(status, processed_at=NOW)

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed) as exc_info:
            payment.fail(LATER)

        assert exc_info.value.context == {"id": PAYMENT_ID, "status": status, "action": "fail"}
        assert payment.status is status


class TestMarkNotified:
    @pytest.mark.parametrize(
        "status", [entities.PaymentStatus.SUCCEEDED, entities.PaymentStatus.FAILED]
    )
    def test__mark_notified(self, status: entities.PaymentStatus) -> None:
        # Setup
        payment = _payment(status, processed_at=NOW)

        # Call
        payment.mark_notified(LATER)

        # Assert
        assert payment.notified_at == LATER
        assert payment.status is status

    def test__pending(self) -> None:
        # Setup
        payment = _payment()

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed) as exc_info:
            payment.mark_notified(NOW)

        assert exc_info.value.context == {
            "id": PAYMENT_ID,
            "status": entities.PaymentStatus.PENDING,
            "action": "notify",
        }
        assert payment.notified_at is None


class TestIsFinal:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (entities.PaymentStatus.PENDING, False),
            (entities.PaymentStatus.SUCCEEDED, True),
            (entities.PaymentStatus.FAILED, True),
        ],
    )
    def test__is_final(self, status: entities.PaymentStatus, expected: bool) -> None:
        # Assert
        assert _payment(status).is_final is expected


class TestMatches:
    def test__same_request(self) -> None:
        # Setup
        payment = _payment()

        # Call and Assert
        assert payment.matches(
            amount=Decimal("10.0"),  # число равно, форма записи другая
            currency=entities.Currency.RUB,
            description="Заказ 1",
            metadata={"tags": ["a", "b"], "order": 1},  # порядок ключей не важен
            webhook_url="http://webhook_sink:8080/hook",
        )

    @pytest.mark.parametrize(
        "changed",
        [
            {"amount": Decimal("10.01")},
            {"currency": entities.Currency.USD},
            {"description": "Заказ 2"},
            {"metadata": {"order": 2, "tags": ["a", "b"]}},
            {"metadata": {"order": 1, "tags": ["b", "a"]}},  # порядок в списке значим
            {"webhook_url": "http://webhook_sink:8080/other"},
        ],
    )
    def test__different_request(self, changed: dict[str, object]) -> None:
        # Setup
        payment = _payment()
        request: dict[str, object] = {
            "amount": Decimal("10.00"),
            "currency": entities.Currency.RUB,
            "description": "Заказ 1",
            "metadata": {"order": 1, "tags": ["a", "b"]},
            "webhook_url": "http://webhook_sink:8080/hook",
        }
        request.update(changed)

        # Call and Assert
        assert not payment.matches(**request)  # type: ignore[arg-type]


class TestDefaults:
    def test__new_payment(self) -> None:
        # Call
        payment = entities.Payment(
            idempotency_key="key-1",
            amount=Decimal("1.00"),
            currency=entities.Currency.EUR,
            webhook_url="https://example.com/hook",
        )

        # Assert
        assert payment.status is entities.PaymentStatus.PENDING
        assert payment.description == ""
        assert payment.metadata == {}
        assert payment.processed_at is None
        assert payment.notified_at is None
        assert payment.created_at.tzinfo is not None
        assert (
            entities.Payment(
                idempotency_key="key-2",
                amount=Decimal("1.00"),
                currency=entities.Currency.EUR,
                webhook_url="https://example.com/hook",
            ).id
            != payment.id
        )
