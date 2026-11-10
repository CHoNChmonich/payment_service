"""Интеграционные тесты адаптера БД на реальном PostgreSQL.

Пропускаются, если не заданы ``TEST_POSTGRES_*`` (читаются из окружения и ``.env``; на
dev-стенде это контейнер ``db_for_tests``). Схема создаётся ``metadata.create_all`` без
миграций и удаляется после теста; каждый тест работает в своей сессии, которая откатывается
по завершении. Схема пересоздаётся на каждый тест, а не на сессию pytest: цикл событий
pytest-asyncio у тестов function-scoped. Пул отключён — у каждой сессии своё соединение,
это нужно тестам на блокировки и обрыв соединения.
"""

from collections.abc import AsyncIterator
from functools import partial

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from payment_service.adapters import database  # импорт пакета регистрирует маппинг
from payment_service.application import interfaces


class DatabaseTestSettings(database.Settings):
    # Те же поля и database_url, что у адаптера, но из переменных с префиксом TEST_
    model_config = SettingsConfigDict(env_prefix="TEST_")


@pytest.fixture(scope="session")
def test_database_settings() -> database.Settings:
    try:
        return DatabaseTestSettings()
    except ValidationError:
        pytest.skip("TEST_POSTGRES_* не заданы — интеграционные тесты БД пропущены")


@pytest.fixture(scope="session")
def test_database_url(test_database_settings: database.Settings) -> str:
    return test_database_settings.database_url


@pytest.fixture(scope="function")
async def create_test_db(test_database_url: str) -> AsyncIterator[AsyncEngine]:
    # Создаём схему тестовой БД и возвращаем движок для использования в тестах
    engine = create_async_engine(test_database_url, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(database.metadata.drop_all)
        await connection.run_sync(database.metadata.create_all)

    yield engine

    # Удаляем схему тестовой БД после теста
    async with engine.begin() as connection:
        await connection.run_sync(database.metadata.drop_all)
    await engine.dispose()


@pytest.fixture(scope="function")
def session_factory(create_test_db: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # Та же конфигурация сессии, что в database.Database: без истечения после commit
    return async_sessionmaker(create_test_db, expire_on_commit=False)


@pytest.fixture(scope="function")
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    # Сессия теста: репозитории работают в ней же, всё незакоммиченное откатывается
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture(scope="function")
def uow_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> interfaces.UnitOfWorkFactory:
    return partial(database.UnitOfWork, session_factory)
