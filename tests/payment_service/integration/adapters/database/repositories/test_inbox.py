import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.adapters.database import repositories, tables
from payment_service.application import errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
FINGERPRINT = "1:" + "a" * 64
OTHER_FINGERPRINT = "1:" + "b" * 64


@pytest.fixture(scope="function")
def repo(session: AsyncSession) -> repositories.InboxRepo:
    return repositories.InboxRepo(session)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRegister:
    async def test__first_registration(
        self, repo: repositories.InboxRepo, session: AsyncSession
    ) -> None:
        # Setup
        message_id = uuid4()

        # Call
        registered = await repo.register(message_id, FINGERPRINT)

        # Assert
        assert registered is True
        stored = await session.scalar(
            select(tables.inbox.c.fingerprint).where(tables.inbox.c.message_id == message_id)
        )
        assert stored == FINGERPRINT

    async def test__repeat_with_same_fingerprint_is_duplicate(
        self, repo: repositories.InboxRepo
    ) -> None:
        # Setup
        message_id = uuid4()
        await repo.register(message_id, FINGERPRINT)

        # Call and Assert
        assert await repo.register(message_id, FINGERPRINT) is False

    async def test__repeat_with_other_fingerprint_conflicts(
        self, repo: repositories.InboxRepo
    ) -> None:
        # Setup
        message_id = uuid4()
        await repo.register(message_id, FINGERPRINT)

        # Call and Assert
        with pytest.raises(errors.MessageConflict) as excinfo:
            await repo.register(message_id, OTHER_FINGERPRINT)
        assert excinfo.value.context == {"message_id": str(message_id)}


class TestConcurrentRegistration:
    @pytest.mark.parametrize(("finish", "expected"), [("commit", False), ("rollback", True)])
    async def test__waits_for_first_transaction(
        self,
        finish: str,
        expected: bool,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        # Setup: первая транзакция зарегистрировала сообщение и ещё не завершилась
        message_id = uuid4()
        async with session_factory() as first, session_factory() as second:
            assert await repositories.InboxRepo(first).register(message_id, FINGERPRINT)
            await second.execute(text("SET LOCAL lock_timeout = '10s'"))

            # Call: вторая регистрация того же ключа ждёт исхода первой
            waiting = asyncio.create_task(
                repositories.InboxRepo(second).register(message_id, FINGERPRINT)
            )
            await asyncio.sleep(0.3)
            assert not waiting.done()
            await (first.commit() if finish == "commit" else first.rollback())

            # Assert: после фиксации первой — дубль, после отката — регистрация проходит
            assert await asyncio.wait_for(waiting, 10) is expected
            await second.rollback()
            async with session_factory() as cleanup:
                await cleanup.execute(
                    tables.inbox.delete().where(tables.inbox.c.message_id == message_id)
                )
                await cleanup.commit()
