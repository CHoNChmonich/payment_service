"""Общий базовый класс репозиториев SQLAlchemy."""

from sqlalchemy.ext.asyncio import AsyncSession


class BaseRepository:
    """Базовый репозиторий: работает в сессии, выданной Unit of Work, и живёт одну транзакцию.

    Паттерн: Adapter (GoF, в широком смысле) — репозиторий приспосабливает ``AsyncSession``
    к контракту порта ядра.

    Attributes:
        session: Сессия транзакции, общая для всех репозиториев одного Unit of Work.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Привязывает репозиторий к сессии транзакции.

        Args:
            session: Сессия, выданная Unit of Work.
        """
        self.session = session
