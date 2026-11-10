"""Репозиторий платежей на SQLAlchemy."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from payment_service.application import entities, interfaces

from .. import tables
from .base import BaseRepository


class PaymentsRepo(BaseRepository, interfaces.PaymentsRepo):
    """Реализация порта платежей поверх ``AsyncSession``."""

    async def add(self, payment: entities.Payment) -> bool:
        """Вставляет платёж через ``ON CONFLICT (idempotency_key) DO NOTHING``.

        Вставка того же ключа из параллельной транзакции ждёт её исхода (уникальный индекс):
        после фиксации первой — конфликт и ``False``, после отката — вставка проходит.
        Ожидание ограничивает ``lock_timeout`` соединения, если он задан. Вставка идёт через
        Core, а не через ``session.add``: ORM не умеет «вставить, если нет».

        Args:
            payment: Новый платёж с присвоенным ``id``.

        Returns:
            ``True`` — платёж вставлен; ``False`` — ключ уже занят, ничего не записано.
        """
        inserted = await self.session.scalar(
            insert(tables.payments)
            .values(
                id=payment.id,
                idempotency_key=payment.idempotency_key,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata,
                webhook_url=payment.webhook_url,
                status=payment.status,
                created_at=payment.created_at,
                processed_at=payment.processed_at,
                notified_at=payment.notified_at,
            )
            .on_conflict_do_nothing(index_elements=[tables.payments.c.idempotency_key])
            .returning(tables.payments.c.id)
        )
        return inserted is not None

    async def fetch_by_id(
        self, payment_id: UUID, *, for_update: bool = False
    ) -> entities.Payment | None:
        """Находит платёж по идентификатору.

        При ``for_update`` запрос идёт с ``populate_existing``: платёж, прочитанный ранее в
        этой сессии без блокировки, перечитывается из БД — под блокировкой состояние должно
        быть актуальным.

        Args:
            payment_id: Идентификатор платежа.
            for_update: Заблокировать строку до конца транзакции (``SELECT … FOR UPDATE``).

        Returns:
            Платёж или ``None``, если такого нет.
        """
        query = select(entities.Payment).where(tables.payments.c.id == payment_id)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        return (await self.session.execute(query)).scalar_one_or_none()

    async def fetch_by_idempotency_key(self, idempotency_key: str) -> entities.Payment | None:
        """Находит платёж по ключу идемпотентности.

        Args:
            idempotency_key: Ключ идемпотентности запроса на создание.

        Returns:
            Платёж или ``None``, если под этим ключом ничего не создано.
        """
        query = select(entities.Payment).where(tables.payments.c.idempotency_key == idempotency_key)
        return (await self.session.execute(query)).scalar_one_or_none()
