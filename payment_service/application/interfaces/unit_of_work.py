"""Порт Unit of Work и тип его фабрики.

Фабрика — не класс, а любой вызываемый объект без аргументов, возвращающий новый
``UnitOfWork``; чем он будет, решает composition root.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from types import TracebackType
from typing import Self

from .outbox import OutboxRepo
from .payments import PaymentsRepo


class UnitOfWork(ABC):
    """Одна транзакция над несколькими репозиториями (паттерн Unit of Work, PoEAA).

    Репозитории разделяют одну сессию хранилища и живут ровно одну транзакцию. Сервис
    открывает единицу работы через ``async with``, работает с репозиториями и завершает запись
    явным ``commit()``; чтения commit не вызывают. ``__aexit__`` реализации обязан откатить всё
    незакоммиченное, закрыть сессию и перевести инфраструктурные сбои хранилища в
    ``errors.StorageUnavailable``.

    Сущности, полученные из репозиториев, действительны только внутри контекста: откат
    при выходе сбрасывает их состояние. Поэтому сервисы собирают DTO до выхода из контекста
    и наружу отдают именно их.

    Репозитории объявлены абстрактными свойствами, а не аннотациями: только так
    ``Mock(spec=UnitOfWork)`` в тестах видит эти атрибуты.
    """

    @property
    @abstractmethod
    def payments(self) -> PaymentsRepo:
        """Репозиторий платежей этой транзакции."""

    @property
    @abstractmethod
    def outbox(self) -> OutboxRepo:
        """Исходящие события этой транзакции: публикуются, только если она зафиксирована."""

    @abstractmethod
    async def __aenter__(self) -> Self:
        """Начинает единицу работы: создаёт её репозитории.

        Returns:
            Сам Unit of Work.
        """

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Завершает единицу работы: откатывает незакоммиченное и освобождает ресурсы.

        Args:
            exc_type: Тип исключения, вышедшего из тела контекста, либо ``None``.
            exc: Само исключение либо ``None``.
            tb: Трассировка исключения либо ``None``.

        Raises:
            StorageUnavailable: Исходное исключение или сбой отката — временный сбой хранилища.
        """

    @abstractmethod
    async def commit(self) -> None:
        """Фиксирует транзакцию; вызывается сервисом в конце записи."""


type UnitOfWorkFactory = Callable[[], UnitOfWork]
