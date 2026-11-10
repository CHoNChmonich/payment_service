from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import Table, func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.adapters.database import tables
from payment_service.application import entities

WEBHOOK_URL = "http://webhook_sink:8080/hook"

PAYMENTS_DATA = [
    {
        "idempotency_key": "key-1",
        "amount": Decimal("10.00"),
        "currency": entities.Currency.RUB,
        "description": "Платёж 1",
        "metadata": {"order": 1},
        "webhook_url": WEBHOOK_URL,
        "status": entities.PaymentStatus.PENDING,
        "created_at": datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
        "processed_at": None,
        "notified_at": None,
    },
    {
        "idempotency_key": "key-2",
        "amount": Decimal("2.50"),
        "currency": entities.Currency.USD,
        "description": "",
        "metadata": {},
        "webhook_url": WEBHOOK_URL,
        "status": entities.PaymentStatus.SUCCEEDED,
        "created_at": datetime(2026, 10, 2, 12, 0, tzinfo=UTC),
        "processed_at": datetime(2026, 10, 2, 12, 0, 5, tzinfo=UTC),
        "notified_at": None,
    },
    {
        "idempotency_key": "key-3",
        "amount": Decimal("100.00"),
        "currency": entities.Currency.EUR,
        "description": "Платёж 3",
        "metadata": {"tags": ["a", "b"], "nested": {"x": None}},
        "webhook_url": WEBHOOK_URL,
        "status": entities.PaymentStatus.FAILED,
        "created_at": datetime(2026, 10, 3, 12, 0, tzinfo=UTC),
        "processed_at": datetime(2026, 10, 3, 12, 0, 5, tzinfo=UTC),
        "notified_at": datetime(2026, 10, 3, 12, 0, 6, tzinfo=UTC),
    },
]


async def insert_payments(session: AsyncSession) -> list[UUID]:
    """Вставляет данные в таблицу `payments`; идентификаторы генерируются здесь."""
    rows = [{**payment, "id": uuid4()} for payment in PAYMENTS_DATA]
    payments_insert = insert(tables.payments).values(rows).returning(tables.payments.c.id)
    return list((await session.execute(payments_insert)).scalars().all())


async def count_rows(table: Table, session: AsyncSession) -> int:
    """Количество строк в таблице."""
    count = (await session.execute(select(func.count()).select_from(table))).scalar_one()
    return int(count)


async def select_status(payment_id: UUID, session: AsyncSession) -> str:
    """Статус платежа из БД (Core-запрос, минуя identity map сессии)."""
    query = select(tables.payments.c.status).where(tables.payments.c.id == payment_id)
    return str((await session.execute(query)).scalar_one())
