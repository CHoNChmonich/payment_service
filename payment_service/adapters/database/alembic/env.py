"""Окружение Alembic для async-движка (asyncpg). Конфиг собирает ``launchers/alembic_runner``."""

import asyncio

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from payment_service.adapters.database.tables import metadata

config = context.config
target_metadata = metadata


def run_migrations_offline() -> None:
    """Генерирует SQL миграций без подключения к БД (режим ``--sql``)."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_migrations(connection: Connection) -> None:
    """Выполняет миграции на синхронном соединении, которое отдал async-движок.

    Args:
        connection: Соединение, полученное через ``AsyncConnection.run_sync``.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Выполняет миграции на живой БД через async-движок без пула соединений."""
    engine = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with engine.connect() as connection:
        await connection.run_sync(_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
