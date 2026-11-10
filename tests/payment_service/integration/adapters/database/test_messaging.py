"""Outbox в сценариях на реальном PostgreSQL: атомарность, гонка создания, доставка relay.

Транзакции коммитятся по-настоящему, как в ``test_unit_of_work``: схема удаляется после теста
(``create_test_db``).
"""

import asyncio
from collections.abc import Sequence
from decimal import Decimal
from unittest.mock import Mock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.adapters import database
from payment_service.adapters.database import tables
from payment_service.application import dtos, entities, errors, interfaces, services

from . import test_data
from .test_unit_of_work import _wait_for_lock_waiter

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
WAIT_TIMEOUT = 15  # секунд; защищает прогон от зависания, если участник гонки упадёт


@pytest.fixture(scope="function", autouse=True)
def _schema(create_test_db: AsyncEngine) -> None:
    pass


@pytest.fixture(scope="function")
def service(uow_factory: interfaces.UnitOfWorkFactory) -> services.Payment:
    return services.Payment(
        uow_factory=uow_factory,
        gateway=Mock(interfaces.PaymentGateway),
        notifier=Mock(interfaces.WebhookNotifier),
    )


class RecordingPublisher(interfaces.EventPublisher):
    """Публикатор-заглушка: запоминает пачки; может ждать разрешения или падать."""

    def __init__(self) -> None:
        self.batches: list[list[dtos.PaymentCreated]] = []
        self.entered = asyncio.Event()
        self.release: asyncio.Event | None = None
        self.error: Exception | None = None

    async def publish(self, events: Sequence[dtos.PaymentCreated]) -> None:
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        self.batches.append(list(events))


class FailingCommitUnitOfWork(database.UnitOfWork):
    """UoW, у которого фиксация падает уже после всей работы сценария."""

    async def commit(self) -> None:
        raise RuntimeError("сбой фиксации")


class PausedCommitUnitOfWork(database.UnitOfWork):
    """UoW, который перед фиксацией ждёт разрешения: строка вставлена, транзакция открыта."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        before_commit: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        super().__init__(session_factory)
        self._before_commit = before_commit
        self._release = release

    async def commit(self) -> None:
        self._before_commit.set()
        await self._release.wait()
        await super().commit()


def _new_payment_info(idempotency_key: str, amount: str = "10.00") -> dtos.NewPaymentInfo:
    return dtos.NewPaymentInfo.model_validate(
        {
            "idempotency_key": idempotency_key,
            "amount": Decimal(amount),
            "currency": "RUB",
            "webhook_url": test_data.WEBHOOK_URL,
        }
    )


async def _race(
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    service: services.Payment,
    first: dtos.NewPaymentInfo,
    second: dtos.NewPaymentInfo,
) -> tuple[dtos.PaymentInfo, dtos.PaymentInfo | BaseException]:
    """Первый ``create`` вставил строку и ждёт перед commit; второй с тем же ключом в это время
    встаёт в очередь на уникальном индексе. Затем первый фиксируется.

    Returns:
        Результат первого и исход второго (платёж или исключение).
    """
    before_commit, release = asyncio.Event(), asyncio.Event()
    paused = services.Payment(
        uow_factory=lambda: PausedCommitUnitOfWork(session_factory, before_commit, release),
        gateway=Mock(interfaces.PaymentGateway),
        notifier=Mock(interfaces.WebhookNotifier),
    )
    first_task = asyncio.create_task(paused.create(first))
    second_task: asyncio.Task[dtos.PaymentInfo] | None = None
    try:
        async with asyncio.timeout(WAIT_TIMEOUT):
            await before_commit.wait()
            second_task = asyncio.create_task(service.create(second))
            await _wait_for_lock_waiter(engine)  # второй ждёт исхода первого, а не читает
            release.set()
            created = await first_task
            outcome = (await asyncio.gather(second_task, return_exceptions=True))[0]
    finally:
        # Сбой посреди гонки не должен оставить открытую транзакцию с блокировкой
        release.set()
        for task in (first_task, second_task):
            if task is not None:
                task.cancel()
    return created, outcome


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestOutboxAtomicity:
    async def test__failed_commit_leaves_neither_payment_nor_event(
        self, session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
    ) -> None:
        # Setup
        failing = services.Payment(
            uow_factory=lambda: FailingCommitUnitOfWork(session_factory),
            gateway=Mock(interfaces.PaymentGateway),
            notifier=Mock(interfaces.WebhookNotifier),
        )

        # Call and Assert
        with pytest.raises(RuntimeError, match="сбой фиксации"):
            await failing.create(_new_payment_info("key-1"))

        assert await test_data.count_rows(tables.payments, session) == 0
        assert await test_data.count_rows(tables.outbox, session) == 0


class TestCreateRace:
    async def test__concurrent_same_key_creates_one_payment(
        self,
        service: services.Payment,
        session_factory: async_sessionmaker[AsyncSession],
        create_test_db: AsyncEngine,
        session: AsyncSession,
    ) -> None:
        """Два запроса с одним ключом одновременно: второй ждёт исхода первого на уникальном
        индексе и получает тот же платёж; событие одно."""
        # Setup
        info = _new_payment_info("race-key")

        # Call
        created, repeated = await _race(session_factory, create_test_db, service, info, info)

        # Assert
        assert repeated == created
        assert await test_data.count_rows(tables.payments, session) == 1
        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__concurrent_same_key_other_content_conflicts(
        self,
        service: services.Payment,
        session_factory: async_sessionmaker[AsyncSession],
        create_test_db: AsyncEngine,
        session: AsyncSession,
    ) -> None:
        # Call
        created, outcome = await _race(
            session_factory,
            create_test_db,
            service,
            _new_payment_info("race-key", "10.00"),
            _new_payment_info("race-key", "20.00"),
        )

        # Assert: первый создан, второй после его фиксации получил конфликт
        assert created.amount == Decimal("10.00")
        assert isinstance(outcome, errors.IdempotencyKeyConflict)
        assert await test_data.count_rows(tables.payments, session) == 1


class TestDeadline:
    async def test__deadline_mid_lock_rolls_back_and_repeat_succeeds(
        self,
        service: services.Payment,
        session_factory: async_sessionmaker[AsyncSession],
        create_test_db: AsyncEngine,
        session: AsyncSession,
    ) -> None:
        """Дедлайн попытки консьюмера истёк, пока сценарий ждал блокировку платежа."""
        # Setup
        created = await service.create(_new_payment_info("key-1"))

        # Call: строку держит другая транзакция; обработка отменяется по дедлайну
        async with asyncio.timeout(WAIT_TIMEOUT), session_factory() as holder:
            await holder.execute(
                select(tables.payments.c.id)
                .where(tables.payments.c.id == created.id)
                .with_for_update()
            )
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(1.0):
                    await _wait_for_lock_waiter_or_process(service, created, create_test_db)
            await holder.rollback()
        async with asyncio.timeout(WAIT_TIMEOUT):  # утёкшая транзакция держала бы строку
            repeated = await service.process(created.id)

        # Assert: отменённая транзакция откатилась — повтор обработал платёж
        assert repeated.status is entities.PaymentStatus.SUCCEEDED
        assert await test_data.select_status(created.id, session) == "succeeded"


async def _wait_for_lock_waiter_or_process(
    service: services.Payment, created: dtos.PaymentInfo, engine: AsyncEngine
) -> None:
    """Запускает обработку и убеждается, что она встала в очередь на блокировку."""
    task = asyncio.create_task(service.process(created.id))
    try:
        await _wait_for_lock_waiter(engine)
        await task
    finally:
        task.cancel()


class TestEventRelay:
    async def test__delivers_in_order_and_clears_outbox(
        self,
        service: services.Payment,
        uow_factory: interfaces.UnitOfWorkFactory,
        session: AsyncSession,
    ) -> None:
        # Setup
        first = await service.create(_new_payment_info("key-1"))
        second = await service.create(_new_payment_info("key-2"))
        third = await service.create(_new_payment_info("key-3"))
        publisher = RecordingPublisher()
        relay = services.EventRelay(uow_factory, publisher, batch_size=2)

        # Call
        published = [await relay.relay() for _ in range(3)]

        # Assert
        assert published == [2, 1, 0]
        assert [[event.payment_id for event in batch] for batch in publisher.batches] == [
            [first.id, second.id],
            [third.id],
        ]
        assert await test_data.count_rows(tables.outbox, session) == 0

    async def test__failed_publication_keeps_events(
        self,
        service: services.Payment,
        uow_factory: interfaces.UnitOfWorkFactory,
        session: AsyncSession,
    ) -> None:
        # Setup
        await service.create(_new_payment_info("key-1"))
        publisher = RecordingPublisher()
        publisher.error = errors.BrokerUnavailable()
        relay = services.EventRelay(uow_factory, publisher, batch_size=10)

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable):
            await relay.relay()

        assert await test_data.count_rows(tables.outbox, session) == 1

    async def test__second_relay_yields_while_first_publishes(
        self, service: services.Payment, uow_factory: interfaces.UnitOfWorkFactory
    ) -> None:
        # Setup
        await service.create(_new_payment_info("key-1"))
        slow = RecordingPublisher()
        slow.release = asyncio.Event()
        fast = RecordingPublisher()
        first = services.EventRelay(uow_factory, slow, batch_size=10)
        second = services.EventRelay(uow_factory, fast, batch_size=10)

        # Call
        async with asyncio.timeout(WAIT_TIMEOUT):
            first_task = asyncio.create_task(first.relay())
            await slow.entered.wait()  # первый держит транзакцию публикатора
            second_result = await second.relay()
            slow.release.set()
            first_result = await first_task

        # Assert: второй не ждёт и не берёт те же события
        assert (first_result, second_result) == (1, 0)
        assert fast.batches == []
