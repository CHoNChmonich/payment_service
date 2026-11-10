from datetime import UTC, datetime
from decimal import Decimal
from random import Random
from unittest.mock import Mock
from uuid import UUID

import pytest

from payment_service.adapters import payment_gateway, retry
from payment_service.application import dtos, entities, errors, interfaces

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT = dtos.PaymentInfo(
    id=UUID("11111111-1111-1111-1111-111111111111"),
    idempotency_key="key-1",
    amount=Decimal("10.00"),
    currency=entities.Currency.RUB,
    description="",
    metadata={},
    webhook_url="http://sink/hook",
    status=entities.PaymentStatus.PENDING,
    created_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    processed_at=None,
    notified_at=None,
)


class FakeRandom(Random):
    """Источник случайности с заданными ответами: задержка и жребий отказа."""

    def __init__(self, *, delay: float, rolls: list[float]) -> None:
        super().__init__(0)
        self.delay = delay
        self.rolls = list(rolls)
        self.ranges: list[tuple[float, float]] = []

    def uniform(self, a: float, b: float) -> float:
        self.ranges.append((a, b))
        return self.delay

    def random(self) -> float:
        return self.rolls.pop(0)


def _settings(**overrides: float) -> payment_gateway.Settings:
    values: dict[str, object] = {
        "PAYMENT_GATEWAY_DELAY_MIN": 2,
        "PAYMENT_GATEWAY_DELAY_MAX": 5,
        "PAYMENT_GATEWAY_FAILURE_RATE": 0.1,
    }
    return payment_gateway.Settings.model_validate(values | overrides)


@pytest.fixture(scope="function")
def sleeps() -> list[float]:
    return []


@pytest.fixture(scope="function")
def sleep(sleeps: list[float]) -> retry.Sleep:
    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    return fake_sleep


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCharge:
    async def test__waits_within_bounds_then_succeeds(
        self, sleep: retry.Sleep, sleeps: list[float]
    ) -> None:
        # Setup: жребий 0.5 ≥ доля отказов 0.1 — успех
        random = FakeRandom(delay=3.3, rolls=[0.5])
        gateway = payment_gateway.PaymentGateway(_settings(), random=random, sleep=sleep)

        # Call
        await gateway.charge(PAYMENT)

        # Assert
        assert random.ranges == [(2, 5)]
        assert sleeps == [3.3]

    async def test__fails_with_configured_rate(self, sleep: retry.Sleep) -> None:
        # Setup: жребий 0.05 < 0.1 — отказ
        random = FakeRandom(delay=2.0, rolls=[0.05])
        gateway = payment_gateway.PaymentGateway(_settings(), random=random, sleep=sleep)

        # Call and Assert
        with pytest.raises(errors.GatewayUnavailable):
            await gateway.charge(PAYMENT)

    async def test__zero_rate_never_fails(self, sleep: retry.Sleep) -> None:
        # Setup: жребий 0.0 при доле 0 — строгое «меньше» не срабатывает
        random = FakeRandom(delay=2.0, rolls=[0.0])
        settings = _settings(PAYMENT_GATEWAY_FAILURE_RATE=0)
        gateway = payment_gateway.PaymentGateway(settings, random=random, sleep=sleep)

        # Call: не поднимает
        await gateway.charge(PAYMENT)

    async def test__full_rate_always_fails(self, sleep: retry.Sleep) -> None:
        # Setup
        random = FakeRandom(delay=2.0, rolls=[0.999])
        settings = _settings(PAYMENT_GATEWAY_FAILURE_RATE=1)
        gateway = payment_gateway.PaymentGateway(settings, random=random, sleep=sleep)

        # Call and Assert
        with pytest.raises(errors.GatewayUnavailable):
            await gateway.charge(PAYMENT)


class TestRetryingPaymentGateway:
    async def test__retries_gateway_unavailability(self, sleep: retry.Sleep) -> None:
        # Setup: два отказа, третья попытка успешна (паузы и исчерпание — test_retry.py)
        inner = Mock(interfaces.PaymentGateway)
        inner.charge.side_effect = [errors.GatewayUnavailable(), errors.GatewayUnavailable(), None]
        gateway = payment_gateway.RetryingPaymentGateway(
            inner, retry.Retry(attempts=3, backoff=1), sleep=sleep
        )

        # Call
        await gateway.charge(PAYMENT)

        # Assert
        assert inner.charge.await_count == 3


class TestSettings:
    def test__worst_case_counts_all_attempts_and_pauses(self) -> None:
        # Setup: 3 попытки по 5 с и паузы 1 + 2 с
        settings = _settings(PAYMENT_GATEWAY_RETRY_ATTEMPTS=3, PAYMENT_GATEWAY_RETRY_BACKOFF=1)

        # Assert
        assert settings.worst_case == 18.0
        assert settings.retry_policy.attempts == 3

    def test__delay_range_is_validated(self) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="DELAY_MIN"):
            _settings(PAYMENT_GATEWAY_DELAY_MIN=6)
