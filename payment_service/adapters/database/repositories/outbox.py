"""Репозиторий outbox на SQLAlchemy Core: события — не сущности, маппинг им не нужен."""

from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import delete, insert, select

from payment_service.application import dtos, interfaces

from .. import tables
from .base import BaseRepository


class OutboxRepo(BaseRepository, interfaces.OutboxRepo):
    """Реализация порта outbox поверх ``AsyncSession``.

    Событие хранится JSON-документом (``model_dump(mode="json")``: UUID и время — строками) и
    восстанавливается моделью события.
    """

    async def add(self, event: dtos.PaymentCreated) -> None:
        """Вставляет событие в транзакцию сессии.

        Args:
            event: Событие сценария.
        """
        await self.session.execute(
            insert(tables.outbox).values(
                event_id=event.event_id, payload=event.model_dump(mode="json")
            )
        )

    async def fetch_batch(self, limit: int) -> list[dtos.PaymentCreated]:
        """Берёт самые ранние события ``FOR UPDATE SKIP LOCKED``.

        Строки, которые держит другая транзакция (другой экземпляр relay), пропускаются:
        два публикатора не возьмут одну пачку и не будут ждать друг друга.

        Args:
            limit: Наибольшее число событий в пачке.

        Returns:
            События в порядке добавления.
        """
        query = (
            select(tables.outbox.c.payload)
            .order_by(tables.outbox.c.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        payloads = (await self.session.execute(query)).scalars()
        return [dtos.PaymentCreated.model_validate(payload) for payload in payloads]

    async def delete(self, event_ids: Sequence[UUID]) -> None:
        """Удаляет события по идентификаторам.

        Args:
            event_ids: Идентификаторы опубликованных событий.
        """
        await self.session.execute(
            delete(tables.outbox).where(tables.outbox.c.event_id.in_(event_ids))
        )
