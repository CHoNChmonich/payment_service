from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.adapters.database import repositories, tables
from payment_service.application import entities

from .. import test_data

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="function")
def repo(session: AsyncSession) -> repositories.PaymentsRepo:
    return repositories.PaymentsRepo(session)


@pytest.fixture(scope="function")
async def fill_db(session: AsyncSession) -> list[UUID]:
    return await test_data.insert_payments(session)


def _new_payment(idempotency_key: str = "new-key") -> entities.Payment:
    return entities.Payment(
        idempotency_key=idempotency_key,
        amount=Decimal("99.99"),
        currency=entities.Currency.EUR,
        description="Новый платёж",
        metadata={"order": 42, "tags": ["x"], "nested": {"flag": True, "none": None}},
        webhook_url=test_data.WEBHOOK_URL,
        created_at=NOW,
    )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestAdd:
    async def test__inserts_and_round_trips(
        self, repo: repositories.PaymentsRepo, session: AsyncSession
    ) -> None:
        # Setup
        payment = _new_payment()

        # Call
        inserted = await repo.add(payment)

        # Assert: все поля вернулись из БД как были, включая JSON и tz-aware время
        assert inserted is True
        session.expunge_all()  # читаем из БД, а не из identity map
        stored = await repo.fetch_by_id(payment.id)
        assert stored is not None
        assert stored is not payment
        assert (stored.idempotency_key, stored.amount, stored.currency) == (
            "new-key",
            Decimal("99.99"),
            entities.Currency.EUR,
        )
        assert (stored.description, stored.metadata, stored.webhook_url) == (
            payment.description,
            payment.metadata,
            payment.webhook_url,
        )
        assert (stored.status, stored.created_at, stored.processed_at, stored.notified_at) == (
            entities.PaymentStatus.PENDING,
            NOW,
            None,
            None,
        )

    async def test__same_key_not_inserted(
        self, repo: repositories.PaymentsRepo, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        duplicate = _new_payment(idempotency_key="key-1")

        # Call
        inserted = await repo.add(duplicate)

        # Assert: ни новой строки, ни ошибки; старый платёж на месте
        assert inserted is False
        assert await test_data.count_rows(tables.payments, session) == len(fill_db)
        assert await repo.fetch_by_id(duplicate.id) is None
        existing = await repo.fetch_by_idempotency_key("key-1")
        assert existing is not None
        assert existing.id == fill_db[0]

    async def test__invalid_currency_rejected_by_db(self, session: AsyncSession) -> None:
        # Call and Assert: CHECK-ограничение, а не только валидация DTO
        with pytest.raises(IntegrityError, match="ck_payments_currency"):
            await session.execute(
                insert(tables.payments).values(
                    {**test_data.PAYMENTS_DATA[0], "id": uuid4(), "currency": "GBP"}
                )
            )

    async def test__invalid_status_rejected_by_db(self, session: AsyncSession) -> None:
        # Call and Assert
        with pytest.raises(IntegrityError, match="ck_payments_payment_status"):
            await session.execute(
                insert(tables.payments).values(
                    {**test_data.PAYMENTS_DATA[0], "id": uuid4(), "status": "unknown"}
                )
            )


class TestFetchById:
    async def test__found(self, repo: repositories.PaymentsRepo, fill_db: list[UUID]) -> None:
        # Call
        payment = await repo.fetch_by_id(fill_db[2])

        # Assert
        assert payment is not None
        assert payment.idempotency_key == "key-3"
        assert payment.status is entities.PaymentStatus.FAILED
        assert payment.metadata == {"tags": ["a", "b"], "nested": {"x": None}}
        assert payment.notified_at == datetime(2026, 10, 3, 12, 0, 6, tzinfo=UTC)

    async def test__not_found(self, repo: repositories.PaymentsRepo, fill_db: list[UUID]) -> None:
        # Call and Assert
        assert await repo.fetch_by_id(uuid4()) is None


class TestFetchByIdempotencyKey:
    async def test__found(self, repo: repositories.PaymentsRepo, fill_db: list[UUID]) -> None:
        # Call
        payment = await repo.fetch_by_idempotency_key("key-2")

        # Assert
        assert payment is not None
        assert payment.id == fill_db[1]
        assert payment.amount == Decimal("2.50")

    async def test__not_found(self, repo: repositories.PaymentsRepo, fill_db: list[UUID]) -> None:
        # Call and Assert
        assert await repo.fetch_by_idempotency_key("missing") is None
