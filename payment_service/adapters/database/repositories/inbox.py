"""Репозиторий inbox на SQLAlchemy Core: регистрация входящих сообщений — не сущность."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from payment_service.application import errors, interfaces

from .. import tables
from .base import BaseRepository


class InboxRepo(BaseRepository, interfaces.InboxRepo):
    """Реализация порта inbox поверх ``AsyncSession``."""

    async def register(self, message_id: UUID, fingerprint: str) -> bool:
        """Вставляет ключ через ``ON CONFLICT DO NOTHING``; при конфликте сверяет отпечаток.

        Вставка того же ключа из параллельной транзакции ждёт её исхода (уникальный индекс):
        после фиксации первой — конфликт, после отката — вставка проходит. Ожидание
        ограничивает ``lock_timeout`` соединения, если он задан.

        Args:
            message_id: Ключ идемпотентности сообщения.
            fingerprint: Отпечаток содержимого сообщения.

        Returns:
            ``True`` — ключ вставлен; ``False`` — ключ уже есть с тем же отпечатком.

        Raises:
            MessageConflict: Ключ уже есть с другим отпечатком.
        """
        inserted = await self.session.scalar(
            insert(tables.inbox)
            .values(message_id=message_id, fingerprint=fingerprint)
            .on_conflict_do_nothing(index_elements=[tables.inbox.c.message_id])
            .returning(tables.inbox.c.message_id)
        )
        if inserted is not None:
            return True
        stored = await self.session.scalar(
            select(tables.inbox.c.fingerprint).where(tables.inbox.c.message_id == message_id)
        )
        if stored == fingerprint:
            return False
        raise errors.MessageConflict(message_id=str(message_id))
