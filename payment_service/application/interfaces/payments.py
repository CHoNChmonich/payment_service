"""Порт персистентности платежей."""

from abc import ABC, abstractmethod
from uuid import UUID

from payment_service.application import entities


class PaymentsRepo(ABC):
    """Порт персистентности платежей."""

    @abstractmethod
    async def add(self, payment: entities.Payment) -> bool:
        """Сохраняет новый платёж, если его ``idempotency_key`` ещё не занят.

        Параллельная вставка того же ключа ждёт исхода первой транзакции: после её фиксации —
        ``False``, после отката — вставка проходит.

        Args:
            payment: Новый платёж с присвоенным ``id``.

        Returns:
            ``True`` — платёж сохранён; ``False`` — платёж с таким ключом уже есть, ничего не
            записано.
        """

    @abstractmethod
    async def fetch_by_id(
        self, payment_id: UUID, *, for_update: bool = False
    ) -> entities.Payment | None:
        """Находит платёж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.
            for_update: Заблокировать строку платежа до конца транзакции; состояние
                перечитывается из БД под блокировкой.

        Returns:
            Платёж или ``None``, если такого нет.
        """

    @abstractmethod
    async def fetch_by_idempotency_key(self, idempotency_key: str) -> entities.Payment | None:
        """Находит платёж по ключу идемпотентности.

        Args:
            idempotency_key: Ключ идемпотентности запроса на создание.

        Returns:
            Платёж или ``None``, если под этим ключом ничего не создано.
        """
