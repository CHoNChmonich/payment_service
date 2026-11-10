"""Unit of Work на реальном PostgreSQL: атомарность, откат, блокировки, сбои хранилища.

В отличие от тестов репозиториев, здесь транзакции коммитятся по-настоящему: данные
готовятся и фиксируются в ``fill_db``, а схема удаляется после теста (``create_test_db``).
"""

import asyncio
from decimal import Decimal
from functools import partial
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from payment_service.adapters import database
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


def _event(payment_id: UUID) -> dtos.PaymentEventInfo:
    return dtos.PaymentEventInfo(event_id=uuid4(), payment_id=payment_id)


async def _registered(event_id: UUID, session: AsyncSession) -> bool:
    """Зарегистрировано ли событие в inbox (зафиксированные данные)."""
    query = select(tables.inbox.c.message_id).where(tables.inbox.c.message_id == event_id)
    return (await session.scalar(query)) is not None


async def _wait_for_lock_waiter(engine: AsyncEngine, statement: str = "") -> None:
    """Дождаться, пока вторая транзакция встанет в очередь на блокировку.

    Args:
        engine: Движок тестовой БД.
        statement: Начало ждущего запроса (например, ``INSERT INTO inbox``); пусто — любой.
    """
    query = text(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND wait_event_type = 'Lock' "
        "AND query ILIKE :prefix"
    ).bindparams(prefix=f"{statement}%")
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
    @pytest.mark.parametrize(
        ("gateway_error", "status"),
        [(None, "succeeded"), (errors.GatewayUnavailable(), "failed")],
        ids=["succeeded", "gateway_refused"],
    )
    async def test__persists_status_event_and_inbox(
        self,
        service: services.Payment,
        gateway: Mock,
        session: AsyncSession,
        fill_db: list[UUID],
        gateway_error: errors.Error | None,
        status: str,
    ) -> None:
        # Setup
        gateway.charge.side_effect = gateway_error
        event = _event(fill_db[0])

        # Call
        applied = await service.process(event)

        # Assert: статус, событие о проведении и регистрация — одной транзакцией
        assert applied is True
        assert await test_data.select_status(fill_db[0], session) == status
        (payload,) = (await session.execute(select(tables.outbox.c.payload))).scalars()
        processed = dtos.PaymentProcessed.model_validate(payload)
        assert processed.payment_id == fill_db[0]
        assert await _registered(event.event_id, session)

    async def test__duplicate_event_is_noop(
        self, service: services.Payment, gateway: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        event = _event(fill_db[0])
        await service.process(event)

        # Call
        again = await service.process(event)

        # Assert: шлюз — один раз, событие о проведении — одно
        assert again is False
        gateway.charge.assert_called_once()
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__rejection_rolls_back_registration(
        self, service: services.Payment, gateway: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        event = _event(fill_db[1])  # платёж уже проведён

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed):
            await service.process(event)

        gateway.charge.assert_not_called()
        assert not await _registered(event.event_id, session)
        assert await test_data.count_rows(tables.outbox, session) == 0


class TestNotify:
    async def test__persists_notified_at_and_inbox(
        self, service: services.Payment, notifier: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        event = _event(fill_db[1])

        # Call
        notified = await service.notify(event)
        again = await service.notify(event)

        # Assert: уведомление отправлено один раз, повтор того же события — холостой
        assert (notified, again) == (True, False)
        notifier.notify.assert_called_once()
        stored = await service.get(fill_db[1])
        assert stored.notified_at is not None
        assert await _registered(event.event_id, session)

    async def test__other_event_for_notified_payment_is_rejected(
        self, service: services.Payment, notifier: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        event = _event(fill_db[2])  # клиент уже уведомлён

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed):
            await service.notify(event)

        notifier.notify.assert_not_called()
        assert not await _registered(event.event_id, session)

    async def test__notifier_failure_leaves_event_for_retry(
        self, service: services.Payment, notifier: Mock, session: AsyncSession, fill_db: list[UUID]
    ) -> None:
        # Setup
        event = _event(fill_db[1])
        notifier.notify.side_effect = [
            errors.WebhookUnavailable(url=test_data.WEBHOOK_URL),
            None,
        ]

        # Call
        with pytest.raises(errors.WebhookUnavailable):
            await service.notify(event)
        stored = await service.get(fill_db[1])
        retried = await service.notify(event)

        # Assert: после сбоя ни отметки, ни регистрации — повтор доставляет
        assert stored.notified_at is None
        assert retried is True
        assert notifier.notify.call_count == 2


class TestConcurrentCopies:
    async def test__copy_of_event_waits_and_is_duplicate(
        self,
        service: services.Payment,
        gateway: Mock,
        create_test_db: AsyncEngine,
        session: AsyncSession,
        fill_db: list[UUID],
    ) -> None:
        """Две копии одного события в параллельных транзакциях: вторая ждёт вставку в inbox
        и после фиксации первой получает «уже обработано», шлюз — один раз."""
        # Setup
        event = _event(fill_db[0])
        charging = asyncio.Event()
        release = asyncio.Event()

        async def charge(payment: dtos.PaymentInfo) -> None:
            charging.set()
            await release.wait()

        gateway.charge.side_effect = charge

        async def copy_after_charge_started() -> bool:
            await charging.wait()
            return await service.process(event)

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            first = asyncio.create_task(service.process(event))
            second = asyncio.create_task(copy_after_charge_started())
            await charging.wait()
            await _wait_for_lock_waiter(create_test_db, "INSERT INTO inbox")  # не строку платежа
            release.set()
            results = (await first, await second)

        # Assert
        assert results == (True, False)
        gateway.charge.assert_called_once()
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__copy_of_notification_waits_and_is_duplicate(
        self,
        service: services.Payment,
        notifier: Mock,
        create_test_db: AsyncEngine,
        fill_db: list[UUID],
    ) -> None:
        """Две копии события «проведён»: webhook уходит один раз, вторая копия — дубль."""
        # Setup
        event = _event(fill_db[1])
        sending = asyncio.Event()
        release = asyncio.Event()

        async def notify(payment: dtos.PaymentInfo) -> None:
            sending.set()
            await release.wait()

        notifier.notify.side_effect = notify

        async def copy_after_send_started() -> bool:
            await sending.wait()
            return await service.notify(event)

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            first = asyncio.create_task(service.notify(event))
            second = asyncio.create_task(copy_after_send_started())
            await sending.wait()
            await _wait_for_lock_waiter(create_test_db, "INSERT INTO inbox")
            release.set()
            results = (await first, await second)

        # Assert
        assert results == (True, False)
        notifier.notify.assert_called_once()

    async def test__copy_waiting_past_lock_timeout_is_storage_unavailable(
        self,
        test_database_url: str,
        uow_factory: interfaces.UnitOfWorkFactory,
        gateway: Mock,
        notifier: Mock,
        fill_db: list[UUID],
    ) -> None:
        """Копия, не дождавшаяся inbox за ``lock_timeout``, — временный сбой (повтор), а не
        дубль и не дефект."""
        # Setup
        event = _event(fill_db[0])
        engine = create_async_engine(
            test_database_url,
            poolclass=NullPool,
            connect_args={"server_settings": {"lock_timeout": "200ms"}},
        )
        impatient = services.Payment(
            uow_factory=partial(
                database.UnitOfWork, async_sessionmaker(engine, expire_on_commit=False)
            ),
            gateway=gateway,
            notifier=notifier,
        )
        charging = asyncio.Event()
        release = asyncio.Event()

        async def charge(payment: dtos.PaymentInfo) -> None:
            charging.set()
            await release.wait()

        holder_gateway = Mock(interfaces.PaymentGateway)
        holder_gateway.charge.side_effect = charge
        holder = services.Payment(
            uow_factory=uow_factory, gateway=holder_gateway, notifier=notifier
        )

        # Call and Assert
        try:
            async with asyncio.timeout(WAIT_TIMEOUT):
                first = asyncio.create_task(holder.process(event))
                await charging.wait()
                with pytest.raises(errors.StorageUnavailable) as exc_info:
                    await impatient.process(event)
                release.set()
                assert await first is True
        finally:
            release.set()
            await engine.dispose()

        assert getattr(getattr(exc_info.value.__cause__, "orig", None), "sqlstate", None) == (
            "55P03"
        )
        gateway.charge.assert_not_called()


class TestForUpdate:
    async def test__processing_waits_for_row_and_sees_final_status(
        self,
        service: services.Payment,
        uow_factory: interfaces.UnitOfWorkFactory,
        gateway: Mock,
        create_test_db: AsyncEngine,
        fill_db: list[UUID],
    ) -> None:
        """Строку держит другая транзакция: обработка ждёт её, видит итог и шлюз не вызывает."""
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

        async def process_after_lock() -> bool:
            await locked.wait()
            return await service.process(_event(payment_id))

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            competitor_task = asyncio.create_task(competitor())
            processing_task = asyncio.create_task(process_after_lock())
            await locked.wait()
            await _wait_for_lock_waiter(create_test_db)  # обработка ждёт строку, а не читает
            release.set()
            await competitor_task
            with pytest.raises(errors.PaymentTransitionNotAllowed):
                await processing_task

        # Assert: шлюз не вызван — платёж уже финальный
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
            _ = uow.inbox
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
