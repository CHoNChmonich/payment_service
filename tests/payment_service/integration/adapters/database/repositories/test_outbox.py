from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.adapters.database import repositories, tables
from payment_service.application import dtos

from .. import test_data


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def repo(session: AsyncSession) -> repositories.OutboxRepo:
    return repositories.OutboxRepo(session)


OCCURRED_AT = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
CREATED: dtos.PaymentEventType = "payment_created"


def _event() -> dtos.PaymentCreated:
    return dtos.PaymentCreated(payment_id=uuid4(), occurred_at=OCCURRED_AT)


def _processed() -> dtos.PaymentProcessed:
    return dtos.PaymentProcessed(payment_id=uuid4(), occurred_at=OCCURRED_AT)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestAdd:
    @pytest.mark.parametrize("make_event", [_event, _processed], ids=[CREATED, "payment_processed"])
    async def test__round_trip(
        self, repo: repositories.OutboxRepo, make_event: Callable[[], dtos.AnyPaymentEvent]
    ) -> None:
        # Setup
        event = make_event()

        # Call
        await repo.add(event)

        # Assert: тип, UUID и время восстановлены из JSON-документа без потерь
        (stored,) = await repo.fetch_batch(10, event.type)
        assert stored == event
        assert type(stored) is type(event)

    async def test__event_id_unique(self, repo: repositories.OutboxRepo) -> None:
        # Setup
        event = _event()
        await repo.add(event)

        # Call and Assert: повторная вставка того же события невозможна
        with pytest.raises(IntegrityError, match="uq_outbox_event_id"):
            await repo.add(event)


class TestFetchBatch:
    async def test__in_insert_order_up_to_limit(self, repo: repositories.OutboxRepo) -> None:
        # Setup
        events = [_event() for _ in range(3)]
        for event in events:
            await repo.add(event)

        # Call
        batch = await repo.fetch_batch(2, CREATED)

        # Assert
        assert batch == events[:2]

    async def test__only_given_type(self, repo: repositories.OutboxRepo) -> None:
        # Setup: типы вперемешку
        created, processed, created_later = _event(), _processed(), _event()
        for event in (created, processed, created_later):
            await repo.add(event)

        # Call
        created_batch = await repo.fetch_batch(10, CREATED)
        processed_batch = await repo.fetch_batch(10, "payment_processed")

        # Assert: пачка — события одного типа в порядке вставки
        assert created_batch == [created, created_later]
        assert processed_batch == [processed]

    async def test__empty(self, repo: repositories.OutboxRepo) -> None:
        # Call and Assert
        assert await repo.fetch_batch(10, CREATED) == []


class TestDelete:
    async def test__deletes_only_given(
        self, repo: repositories.OutboxRepo, session: AsyncSession
    ) -> None:
        # Setup
        first, second, third = (_event() for _ in range(3))
        for event in (first, second, third):
            await repo.add(event)

        # Call
        await repo.delete([first.event_id, third.event_id])

        # Assert
        assert await repo.fetch_batch(10, CREATED) == [second]
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__nothing_to_delete(self, repo: repositories.OutboxRepo) -> None:
        # Call and Assert: пустой список и чужие идентификаторы — не ошибка
        await repo.delete([])
        await repo.delete([uuid4()])
