"""Lifespan приложения из списков хуков запуска и остановки."""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger(__name__)

LifecycleCallback = Callable[[], Awaitable[None]]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_lifespan(
    *,
    setup: list[LifecycleCallback] | None = None,
    cleanup: list[LifecycleCallback] | None = None,
) -> Lifespan:
    """Строит lifespan из хуков запуска и остановки (например, проверка и закрытие БД).

    Ошибка хука запуска роняет приложение — неверная конфигурация должна быть видна сразу;
    ошибка хука остановки только логируется, чтобы остальные хуки отработали.

    Args:
        setup: Хуки, выполняемые по порядку при старте приложения.
        cleanup: Хуки, выполняемые по порядку при остановке приложения.

    Returns:
        Фабрика async-контекста, которую принимает ``FastAPI(lifespan=...)``.
    """
    setup_hooks: list[LifecycleCallback] = setup or []
    cleanup_hooks: list[LifecycleCallback] = cleanup or []

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Выполняет хуки запуска, отдаёт управление приложению, затем хуки остановки.

        Args:
            app: Приложение FastAPI; не используется, сигнатуру задаёт FastAPI.

        Yields:
            Управление на время работы приложения.
        """
        for hook in setup_hooks:
            await hook()
        try:
            yield
        finally:
            for hook in cleanup_hooks:
                try:
                    await hook()
                except Exception:
                    logger.warning("Хук остановки завершился с ошибкой", exc_info=True)

    return lifespan
