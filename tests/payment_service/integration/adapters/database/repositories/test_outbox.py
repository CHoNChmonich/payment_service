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


def _event() -> dtos.PaymentCreated:
    return dtos.PaymentCreated(
        payment_id=uuid4(), occurred_at=datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestAdd:
    async def test__round_trip(self, repo: repositories.OutboxRepo) -> None:
        # Setup
        event = _event()

        # Call
        await repo.add(event)

        # Assert: UUID и время восстановлены из JSON-документа без потерь
        (stored,) = await repo.fetch_batch(10)
        assert stored == event

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
        batch = await repo.fetch_batch(2)

        # Assert
        assert batch == events[:2]

    async def test__empty(self, repo: repositories.OutboxRepo) -> None:
        # Call and Assert
        assert await repo.fetch_batch(10) == []


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
        assert await repo.fetch_batch(10) == [second]
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__nothing_to_delete(self, repo: repositories.OutboxRepo) -> None:
        # Call and Assert: пустой список и чужие идентификаторы — не ошибка
        await repo.delete([])
        await repo.delete([uuid4()])
