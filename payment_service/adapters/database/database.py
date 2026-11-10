"""Движок и фабрика сессий SQLAlchemy; уровни логгеров SQLAlchemy для конфигурации логирования."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .settings import Settings


class Database:
    """Движок и фабрика сессий SQLAlchemy; открывается и закрывается хуками процесса.

    Attributes:
        session_factory: Фабрика сессий с ``expire_on_commit=False``: после commit
            к сущностям можно обращаться без новых запросов.
    """

    def __init__(self, settings: Settings) -> None:
        """Создаёт движок и фабрику сессий; соединения открываются лениво.

        Пул создаётся с ``pool_pre_ping``: мёртвое простаивающее соединение проверяется
        до выдачи из пула, а не на первом запросе транзакции. SQL-лог включается не флагом
        ``echo`` движка, а уровнем логгера (см. ``logger_levels``). ``lock_timeout`` из
        настроек ставится параметром сервера на каждое соединение, предел установки
        соединения — параметром драйвера.

        Args:
            settings: Параметры подключения к PostgreSQL.
        """
        self._settings = settings
        server_settings: dict[str, str] = {}
        if settings.POSTGRES_LOCK_TIMEOUT_MS is not None:
            server_settings["lock_timeout"] = f"{settings.POSTGRES_LOCK_TIMEOUT_MS}ms"
        self._engine: AsyncEngine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
            connect_args={
                "server_settings": server_settings,
                "timeout": settings.POSTGRES_CONNECT_TIMEOUT,
            },
        )
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    @property
    def engine(self) -> AsyncEngine:
        """Движок — для проверок и тестов."""
        return self._engine

    async def aopen(self) -> None:
        """Проверяет доступность БД на старте: ошибка конфигурации — ошибка запуска.

        Raises:
            RuntimeError: К БД не удалось подключиться; причина — в тексте и в ``__cause__``.
        """
        try:
            async with self._engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception as exc:
            raise RuntimeError(
                f"PostgreSQL недоступен по адресу "
                f"{self._settings.POSTGRES_HOST}:{self._settings.POSTGRES_PORT}: {exc}"
            ) from exc

    async def aclose(self) -> None:
        """Закрывает пул соединений при остановке приложения."""
        await self._engine.dispose()


def logger_levels(settings: Settings) -> dict[str, str]:
    """Уровни логгеров SQLAlchemy для ``log.configure``.

    SQL пишется логгером ``sqlalchemy.engine`` на уровне INFO. Включать его через ``echo=True``
    у движка нельзя: ``echo`` вешает на логгер собственный ``StreamHandler``, и при
    настроенном корневом хендлере каждая строка SQL пишется дважды.

    Args:
        settings: Параметры подключения; ``POSTGRES_ECHO_SQL`` включает SQL-лог.

    Returns:
        Имя логгера → уровень.
    """
    return {"sqlalchemy.engine": "INFO" if settings.POSTGRES_ECHO_SQL else "WARNING"}
