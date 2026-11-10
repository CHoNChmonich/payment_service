"""Unit of Work на реальном PostgreSQL: атомарность, откат, блокировки, сбои хранилища.

В отличие от тестов репозиториев, здесь транзакции коммитятся по-настоящему: данные
готовятся и фиксируются в ``fill_db``, а схема удаляется после теста (``create_test_db``).
"""

import asyncio
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from payment_service.adapters.database import tables
from payment_service.application import dtos, entities, errors, interfaces, services

from . import test_data

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
WAIT_TIMEOUT = 15  # секунд; защищает прогон от зависания, если участник гонки упадёт


@pytest.fixture(scope="function", autouse=True)
async def fill_db(session: AsyncSession) -> list[UUID]:
    payment_ids = await test_data.insert_payments(session)
    await session.commit()  # UoW работает в других сессиях — данные должны быть зафиксированы
    return payment_ids


@pytest.fixture(scope="function")
def gateway() -> Mock:
    return Mock(interfaces.PaymentGateway)


@pytest.fixture(scope="function")
def notifier() -> Mock:
    return Mock(interfaces.WebhookNotifier)


@pytest.fixture(scope="function")
def service(
    uow_factory: interfaces.UnitOfWorkFactory, gateway: Mock, notifier: Mock
) -> services.Payment:
    return services.Payment(uow_factory=uow_factory, gateway=gateway, notifier=notifier)


def _new_payment_info(idempotency_key: str = "new-key") -> dtos.NewPaymentInfo:
    return dtos.NewPaymentInfo.model_validate(
        {
            "idempotency_key": idempotency_key,
            "amount": Decimal("10.00"),
            "currency": "RUB",
            "description": "Заказ",
            "metadata": {"order": 1},
            "webhook_url": test_data.WEBHOOK_URL,
        }
    )


async def _wait_for_lock_waiter(engine: AsyncEngine) -> None:
    """Дождаться, пока вторая транзакция встанет в очередь на блокировку строки."""
    query = text(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
    )
    for _ in range(200):
        async with engine.connect() as connection:
            if (await connection.execute(query)).scalar_one():
                return
        await asyncio.sleep(0.05)
    raise AssertionError("вторая транзакция не встала в ожидание блокировки")


async def _terminate_other_backends(engine: AsyncEngine) -> None:
    """Оборвать все остальные соединения с тестовой БД — имитация потери сети."""
    async with engine.connect() as connection:
        await connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid <> pg_backend_pid()"
            )
        )


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCreate:
    async def test__writes_payment_and_event_atomically(
        self, service: services.Payment, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Call
        created = await service.create(_new_payment_info())

        # Assert: в той же транзакции — платёж и событие о нём
        assert await test_data.count_rows(tables.payments, session) == len(fill_db) + 1
        assert await test_data.select_status(created.id, session) == "pending"
        (payload,) = (await session.execute(select(tables.outbox.c.payload))).scalars()
        event = dtos.PaymentCreated.model_validate(payload)
        assert event.payment_id == created.id
        assert event.occurred_at == created.created_at

    async def test__repeat_returns_existing_without_new_event(
        self, service: services.Payment, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        first = await service.create(_new_payment_info())

        # Call
        second = await service.create(_new_payment_info())

        # Assert
        assert second == first
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__same_key_other_content_conflicts(
        self, service: services.Payment, fill_db: list[UUID]
    ) -> None:
        # Setup
        await service.create(_new_payment_info())
        changed = _new_payment_info().model_copy(update={"amount": Decimal("11.00")})

        # Call and Assert
        with pytest.raises(errors.IdempotencyKeyConflict):
            await service.create(changed)


class TestProcess:
    async def test__persists_final_status(
        self, service: services.Payment, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Call
        processed = await service.process(fill_db[0])

        # Assert
        assert processed.status is entities.PaymentStatus.SUCCEEDED
        assert processed.processed_at is not None
        assert await test_data.select_status(fill_db[0], session) == "succeeded"

    async def test__gateway_failure_persists_failed(
        self, service: services.Payment, gateway: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        gateway.charge.side_effect = errors.GatewayUnavailable()

        # Call
        processed = await service.process(fill_db[0])

        # Assert
        assert processed.status is entities.PaymentStatus.FAILED
        assert await test_data.select_status(fill_db[0], session) == "failed"


class TestNotify:
    async def test__persists_notified_at(
        self, service: services.Payment, notifier: Mock, fill_db: list[UUID]
    ) -> None:
        # Call
        notified = await service.notify(fill_db[1])
        again = await service.notify(fill_db[1])

        # Assert: уведомление отправлено один раз, повтор — холостой
        assert (notified, again) == (True, False)
        notifier.notify.assert_called_once()
        stored = await service.get(fill_db[1])
        assert stored.notified_at is not None

    async def test__notifier_failure_leaves_not_notified(
        self, service: services.Payment, notifier: Mock, fill_db: list[UUID]
    ) -> None:
        # Setup
        notifier.notify.side_effect = errors.WebhookUnavailable(url=test_data.WEBHOOK_URL)

        # Call and Assert
        with pytest.raises(errors.WebhookUnavailable):
            await service.notify(fill_db[1])

        stored = await service.get(fill_db[1])
        assert stored.notified_at is None


class TestForUpdate:
    async def test__second_processing_waits_and_sees_final_status(
        self,
        service: services.Payment,
        uow_factory: interfaces.UnitOfWorkFactory,
        gateway: Mock,
        create_test_db: AsyncEngine,
        fill_db: list[UUID],
    ) -> None:
        """Две обработки одного платежа: FOR UPDATE пускает их по очереди, вторая видит итог."""
        # Setup
        payment_id = fill_db[0]
        locked = asyncio.Event()
        release = asyncio.Event()

        async def competitor() -> None:
            async with uow_factory() as uow:
                payment = await uow.payments.fetch_by_id(payment_id, for_update=True)
                assert payment is not None
                locked.set()
                await release.wait()
                payment.fail(payment.created_at)
                await uow.commit()

        async def process_after_lock() -> dtos.PaymentInfo:
            await locked.wait()
            return await service.process(payment_id)

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            competitor_task = asyncio.create_task(competitor())
            processing_task = asyncio.create_task(process_after_lock())
            await locked.wait()
            await _wait_for_lock_waiter(create_test_db)  # обработка ждёт строку, а не читает
            release.set()
            await competitor_task
            outcome = await processing_task

        # Assert: шлюз не вызван — платёж уже финальный
        assert outcome.status is entities.PaymentStatus.FAILED
        gateway.charge.assert_not_called()

    async def test__refreshes_payment_read_earlier_without_lock(
        self, uow_factory: interfaces.UnitOfWorkFactory, fill_db: list[UUID]
    ) -> None:
        """Под блокировкой платёж перечитывается, даже если эта же транзакция уже видела его."""
        # Setup
        payment_id = fill_db[0]

        # Call and Assert
        async with uow_factory() as uow:
            stale = await uow.payments.fetch_by_id(payment_id)  # без блокировки: pending
            assert stale is not None
            async with uow_factory() as other:
                payment = await other.payments.fetch_by_id(payment_id, for_update=True)
                assert payment is not None
                payment.succeed(payment.created_at)
                await other.commit()
            locked = await uow.payments.fetch_by_id(payment_id, for_update=True)
            assert locked is stale
            assert locked.status is entities.PaymentStatus.SUCCEEDED


class TestStorageUnavailable:
    async def test__deadlock(
        self, uow_factory: interfaces.UnitOfWorkFactory, fill_db: list[UUID]
    ) -> None:
        """Захват двух строк в противоположном порядке разными запросами: PostgreSQL прерывает
        одну транзакцию (sqlstate 40P01), UoW переводит это в ``StorageUnavailable``."""
        # Setup
        first_id, second_id, _ = fill_db
        first_locked = asyncio.Event()
        second_locked = asyncio.Event()

        async def lock_in_order(
            first: UUID, second: UUID, mine: asyncio.Event, other: asyncio.Event
        ) -> None:
            async with uow_factory() as uow:
                await uow.payments.fetch_by_id(first, for_update=True)
                mine.set()
                await other.wait()
                await uow.payments.fetch_by_id(second, for_update=True)
                await uow.commit()

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            results = await asyncio.gather(
                lock_in_order(first_id, second_id, first_locked, second_locked),
                lock_in_order(second_id, first_id, second_locked, first_locked),
                return_exceptions=True,
            )

        # Assert
        failures = [result for result in results if isinstance(result, BaseException)]
        assert len(failures) == 1
        assert isinstance(failures[0], errors.StorageUnavailable)
        cause = failures[0].__cause__
        assert getattr(getattr(cause, "orig", None), "sqlstate", None) == "40P01"

    async def test__lost_connection(
        self,
        uow_factory: interfaces.UnitOfWorkFactory,
        create_test_db: AsyncEngine,
        fill_db: list[UUID],
    ) -> None:
        # Call and Assert
        with pytest.raises(errors.StorageUnavailable):
            async with uow_factory() as uow:
                await uow.payments.fetch_by_id(fill_db[0])  # соединение транзакции открыто
                await _terminate_other_backends(create_test_db)  # «сеть упала»
                await uow.payments.fetch_by_id(fill_db[0], for_update=True)

    async def test__lost_connection_on_commit(
        self,
        uow_factory: interfaces.UnitOfWorkFactory,
        create_test_db: AsyncEngine,
        session: AsyncSession,
        fill_db: list[UUID],
    ) -> None:
        # Call and Assert
        with pytest.raises(errors.StorageUnavailable):
            async with uow_factory() as uow:
                payment = await uow.payments.fetch_by_id(fill_db[0], for_update=True)
                assert payment is not None
                payment.succeed(payment.created_at)
                await _terminate_other_backends(create_test_db)  # «сеть упала» перед commit
                await uow.commit()

        assert await test_data.select_status(fill_db[0], session) == "pending"


class TestLifecycle:
    async def test__repositories_unavailable_after_exit(
        self, uow_factory: interfaces.UnitOfWorkFactory
    ) -> None:
        # Call
        async with uow_factory() as uow:
            pass

        # Assert
        with pytest.raises(AttributeError):
            _ = uow.payments
        with pytest.raises(AttributeError):
            _ = uow.outbox
        with pytest.raises(AttributeError):
            await uow.commit()

    async def test__nested_enter_rejected(self, uow_factory: interfaces.UnitOfWorkFactory) -> None:
        # Call and Assert
        async with uow_factory() as uow:
            with pytest.raises(RuntimeError):
                async with uow:
                    pass

    async def test__sequential_reuse_opens_new_session(
        self, uow_factory: interfaces.UnitOfWorkFactory, fill_db: list[UUID]
    ) -> None:
        # Setup
        uow = uow_factory()

        # Call
        async with uow:
            first = await uow.payments.fetch_by_id(fill_db[0])
        async with uow:
            second = await uow.payments.fetch_by_id(fill_db[0])

        # Assert: разные сессии — разные identity map
        assert first is not None
        assert second is not None
        assert first is not second

    async def test__uncommitted_changes_rolled_back(
        self, uow_factory: interfaces.UnitOfWorkFactory, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Call: изменение без commit
        async with uow_factory() as uow:
            payment = await uow.payments.fetch_by_id(fill_db[0], for_update=True)
            assert payment is not None
            payment.succeed(payment.created_at)

        # Assert
        assert await test_data.select_status(fill_db[0], session) == "pending"
