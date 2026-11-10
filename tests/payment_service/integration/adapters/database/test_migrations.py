"""Миграции Alembic против ``metadata`` на реальном PostgreSQL.

Остальные интеграционные тесты строят схему через ``metadata.create_all``, поэтому ограничение,
забытое в миграции (уникальность ``idempotency_key``, CHECK на статус), тесты бы не заметили,
а в рабочей БД исчезла бы гарантия, на которой держится идемпотентность.
"""

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import partial
from itertools import product
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection, func, insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from payment_service.adapters import database
from payment_service.adapters.database import tables
from payment_service.application import dtos, entities, interfaces, services
from payment_service.launchers.alembic_runner import make_config

from . import test_data


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
def _schema_diff(connection: Connection) -> list[object]:
    """Расхождения схемы БД с ``metadata``, включая значения по умолчанию на стороне сервера."""
    context = MigrationContext.configure(connection, opts={"compare_server_default": True})
    return list(compare_metadata(context, database.metadata))


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestMigrations:
    async def test__upgrade_head_matches_metadata_and_guards_rows(
        self, create_test_db: AsyncEngine, test_database_settings: database.Settings
    ) -> None:
        # Setup: пустая БД вместо схемы из create_all
        async with create_test_db.begin() as connection:
            await connection.run_sync(database.metadata.drop_all)
        config = make_config(test_database_settings)
        valid_row = {**test_data.PAYMENTS_DATA[0], "id": uuid4()}
        # По строке на каждую пару валюта × статус: CHECK с неполным списком значений не пройдёт
        valid_rows = [
            {
                **valid_row,
                "id": uuid4(),
                "idempotency_key": f"key-{c}-{s}",
                "currency": c,
                "status": s,
            }
            for c, s in product(entities.Currency, entities.PaymentStatus)
        ]
        invalid_rows = {
            "ck_payments_currency": {**valid_row, "id": uuid4(), "currency": "GBP"},
            "ck_payments_payment_status": {**valid_row, "id": uuid4(), "status": "unknown"},
            "uq_payments_idempotency_key": {**valid_row, "id": uuid4()},
        }

        try:
            # Call: env.py запускает свой цикл событий — миграции идут в отдельном потоке
            await asyncio.to_thread(command.upgrade, config, "head")
            async with create_test_db.connect() as connection:
                diff = await connection.run_sync(_schema_diff)
            # Вставки, как у репозиториев: id outbox и время — значения по умолчанию сервера
            async with create_test_db.begin() as connection:
                await connection.execute(insert(tables.payments).values(valid_row))
                await connection.execute(insert(tables.payments).values(valid_rows))
                await connection.execute(
                    insert(tables.outbox).values(event_id=uuid4(), payload={"type": "x"})
                )
                outbox_id, created_at = (
                    await connection.execute(select(tables.outbox.c.id, tables.outbox.c.created_at))
                ).one()
            rejected: dict[str, str] = {}
            for constraint, row in invalid_rows.items():
                async with create_test_db.connect() as connection:
                    with pytest.raises(IntegrityError) as excinfo:
                        await connection.execute(insert(tables.payments).values(row))
                    rejected[constraint] = str(excinfo.value)

            # Assert
            assert diff == []
            assert outbox_id == 1
            assert created_at.tzinfo is not None
            for constraint, message in rejected.items():
                assert constraint in message
        finally:
            await asyncio.to_thread(command.downgrade, config, "base")
            async with create_test_db.begin() as connection:
                await connection.execute(text("DROP TABLE IF EXISTS alembic_version"))

    async def test__0002_moves_unnotified_payments_between_event_types(
        self, create_test_db: AsyncEngine, test_database_settings: database.Settings
    ) -> None:
        """Платежи, обработанные без уведомления, получают ``payment_processed``, который relay
        публикует и удаляет; откат возвращает им ``payment_created`` для прежнего консьюмера."""
        # Setup: схема 0001 и платежи всех видов: pending, обработан без уведомления, уведомлён,
        # финальный без processed_at (CHECK такую строку допускает)
        async with create_test_db.begin() as connection:
            await connection.run_sync(database.metadata.drop_all)
        config = make_config(test_database_settings)
        pending_id, unnotified_id, without_moment_id = uuid4(), uuid4(), uuid4()
        pending, unnotified, notified = (
            {**row, "id": payment_id}
            for row, payment_id in zip(
                test_data.PAYMENTS_DATA, [pending_id, unnotified_id, uuid4()], strict=True
            )
        )
        # Микросекунды — проверка формата времени в документе, собранном SQL
        unnotified["processed_at"] = datetime(2026, 10, 2, 12, 0, 5, 123456, tzinfo=UTC)
        without_moment = {
            **unnotified,
            "id": without_moment_id,
            "idempotency_key": "key-4",
            "processed_at": None,
        }
        publisher = _RecordingPublisher()
        uow_factory = partial(
            database.UnitOfWork, async_sessionmaker(create_test_db, expire_on_commit=False)
        )
        relay = services.EventRelay(uow_factory, publisher, batch_size=10)

        try:
            await asyncio.to_thread(command.upgrade, config, "0001")
            async with create_test_db.begin() as connection:
                await connection.execute(
                    insert(tables.payments).values([pending, unnotified, notified, without_moment])
                )

            # Call
            await asyncio.to_thread(command.upgrade, config, "head")
            published = (await relay.relay()).published
            async with create_test_db.connect() as connection:
                left_after_relay = await connection.scalar(
                    select(func.count()).select_from(tables.outbox)
                )
            # Перед откатом в outbox — неопубликованные события нового кода: «проведён» и
            # «создан» ожидающего платежа
            pending_created = dtos.PaymentCreated(payment_id=pending_id)
            async with uow_factory() as uow:
                await uow.outbox.add(dtos.PaymentProcessed(payment_id=unnotified_id))
                await uow.outbox.add(pending_created)
                await uow.commit()
            await asyncio.to_thread(command.downgrade, config, "0001")
            async with create_test_db.connect() as connection:
                payloads = (
                    (await connection.execute(select(tables.outbox.c.payload))).scalars().all()
                )

            # Assert: события — только неуведомлённым; relay их опубликовал и удалил (event_id
            # в колонке и в документе совпадает)
            assert published == 2
            events = {event.payment_id: event for event in publisher.events}
            assert set(events) == {unnotified_id, without_moment_id}
            assert all(isinstance(event, dtos.PaymentProcessed) for event in events.values())
            assert events[unnotified_id].occurred_at == unnotified["processed_at"]
            assert events[without_moment_id].occurred_at == without_moment["created_at"]
            assert left_after_relay == 0
            # Откат: «проведён» удалён, «создан» ожидающего платежа сохранён, неуведомлённым —
            # по одному ``payment_created`` в прежнем формате
            restored = [dtos.PaymentCreated.model_validate(payload) for payload in payloads]
            assert pending_created in restored
            assert sorted(str(event.payment_id) for event in restored) == sorted(
                str(payment_id) for payment_id in (pending_id, unnotified_id, without_moment_id)
            )
        finally:
            await asyncio.to_thread(command.downgrade, config, "base")
            async with create_test_db.begin() as connection:
                await connection.execute(text("DROP TABLE IF EXISTS alembic_version"))


class _RecordingPublisher(interfaces.EventPublisher):
    """Публикатор-заглушка: запоминает опубликованные события."""

    def __init__(self) -> None:
        self.events: list[dtos.AnyPaymentEvent] = []

    async def publish(self, events: Sequence[dtos.AnyPaymentEvent]) -> None:
        self.events.extend(events)
