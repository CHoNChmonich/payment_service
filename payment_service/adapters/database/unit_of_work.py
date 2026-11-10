"""Unit of Work поверх ``AsyncSession``: репозитории на одной сессии и одна транзакция."""

import logging
import socket
from types import TracebackType
from typing import Self

from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.application import errors, interfaces

from . import repositories

logger = logging.getLogger(__name__)

# Сбой сериализации, взаимная блокировка, отказ сервера принять соединение (лимит
# соединений, остановка или перезапуск), блокировка не получена за ``lock_timeout``, запрос
# отменён (``statement_timeout``, отмена сервером): транзакцию можно повторить.
_TRANSIENT_SQLSTATES = frozenset(
    {"40001", "40P01", "53300", "55P03", "57014", "57P01", "57P02", "57P03"}
)


class UnitOfWork(interfaces.UnitOfWork):
    """Транзакция SQLAlchemy над репозиториями платежей, outbox и inbox.

    Паттерн: Adapter (GoF, в широком смысле) — приспосабливает ``AsyncSession`` к контракту
    порта ``interfaces.UnitOfWork``. Объект рассчитан на одну единицу работы за раз: вход в
    контекст создаёт сессию и репозитории, выход откатывает незакоммиченное, закрывает сессию
    и удаляет их, поэтому вне контекста репозитории и ``commit`` недоступны
    (``AttributeError``), а вложенный вход — ошибка. Соединение берётся из пула при первом
    запросе (autobegin).
    """

    _session: AsyncSession
    _payments: interfaces.PaymentsRepo
    _outbox: interfaces.OutboxRepo
    _inbox: interfaces.InboxRepo

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        """Запоминает фабрику сессий; сессия не создаётся до входа в контекст.

        Args:
            session_factory: Фабрика сессий из ``Database.session_factory``.
        """
        self._session_factory = session_factory

    @property
    def payments(self) -> interfaces.PaymentsRepo:
        """Репозиторий платежей этой транзакции."""
        return self._payments

    @property
    def outbox(self) -> interfaces.OutboxRepo:
        """Исходящие события этой транзакции."""
        return self._outbox

    @property
    def inbox(self) -> interfaces.InboxRepo:
        """Входящие сообщения, обработанные этой транзакцией."""
        return self._inbox

    async def __aenter__(self) -> Self:
        """Создаёт сессию и репозитории на ней; транзакция начнётся при первом запросе.

        Returns:
            Сам Unit of Work.

        Raises:
            RuntimeError: Единица работы уже открыта — вложенный вход без выхода.
        """
        if hasattr(self, "_session"):
            raise RuntimeError("Unit of Work уже открыт: вложенный вход недопустим")
        self._session = self._session_factory()
        self._payments = repositories.PaymentsRepo(self._session)
        self._outbox = repositories.OutboxRepo(self._session)
        self._inbox = repositories.InboxRepo(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Откатывает незакоммиченное, закрывает и удаляет сессию, переводит сбои хранилища.

        Откат после успешного commit холостой. Это единственное место перевода сбоев
        хранилища (включая ошибку commit) в ошибку ядра: исключение, поднятое здесь,
        заменяет исходное, сохраняя его как причину.

        Args:
            exc_type: Тип исключения, вышедшего из тела контекста, либо ``None``.
            exc: Само исключение либо ``None``.
            tb: Трассировка исключения либо ``None``.

        Raises:
            StorageUnavailable: Исходное исключение или сбой отката — временный сбой хранилища.
        """
        try:
            await self._session.rollback()
        except Exception as rollback_error:
            if self._is_transient(rollback_error):
                raise errors.StorageUnavailable() from rollback_error
            raise
        finally:
            await self._session.close()
            del self._session, self._payments, self._outbox, self._inbox
        if self._is_transient(exc):
            raise errors.StorageUnavailable() from exc

    async def commit(self) -> None:
        """Фиксирует транзакцию; ошибка commit уходит в ``__aexit__`` и переводится там.

        Об исходе фиксации знает только адаптер, поэтому DEBUG-запись о ней живёт здесь,
        а не в сервисах.
        """
        await self._session.commit()
        logger.debug("Транзакция зафиксирована")

    @staticmethod
    def _is_transient(exc: BaseException | None) -> bool:
        """Определяет, является ли исключение временным сбоем хранилища.

        Временными считаются: невозможность установить соединение (отказ или таймаут),
        неразрешимое имя хоста, исчерпание пула, разрыв соединения (диалект asyncpg не
        порождает ``OperationalError`` — обрыв приходит ``InterfaceError`` с флагом
        ``connection_invalidated``, «соединение признано мёртвым»), а также ошибки сервера
        с кодами из ``_TRANSIENT_SQLSTATES``.

        Args:
            exc: Исключение, вышедшее из тела транзакции, отката или commit; ``None``, если
                его нет.

        Returns:
            ``True``, если операцию имеет смысл повторить.
        """
        if isinstance(exc, ConnectionError | TimeoutError | socket.gaierror | PoolTimeoutError):
            return True
        if isinstance(exc, DBAPIError):
            if exc.connection_invalidated or isinstance(exc, OperationalError):
                return True
            return getattr(exc.orig, "sqlstate", None) in _TRANSIENT_SQLSTATES
        return False
