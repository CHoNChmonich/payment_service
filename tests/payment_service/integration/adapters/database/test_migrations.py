"""Миграции Alembic против ``metadata`` на реальном PostgreSQL.

Остальные интеграционные тесты строят схему через ``metadata.create_all``, поэтому ограничение,
забытое в миграции (уникальность ``idempotency_key``, CHECK на статус), тесты бы не заметили,
а в рабочей БД исчезла бы гарантия, на которой держится идемпотентность.
"""

import asyncio
from itertools import product
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection, insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.adapters import database
from payment_service.adapters.database import tables
from payment_service.application import entities
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
