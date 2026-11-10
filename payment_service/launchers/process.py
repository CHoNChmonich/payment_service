"""Запуск фонового asyncio-процесса (consumer, relay): сигналы, хуки, код выхода, метка «жив».

Роль, которую у HTTP-сервиса играют uvicorn и ``api.create_lifespan``: хуки запуска и
остановки с той же семантикой (ошибка запуска роняет процесс, ошибка остановки только
логируется), SIGTERM/SIGINT — событие остановки, которое цикл процесса проверяет между
итерациями; повторный сигнал отменяет цикл, не дожидаясь конца итерации (хуки остановки и
блокирующие вызовы в потоках при этом дорабатывают). Исключение из цикла — код выхода 1:
процесс падает заметно, оркестратор его перезапускает.

Healthcheck контейнера: ``python -m payment_service.launchers.process --check-heartbeat <сек>``
— код 0, если метка «жив» (``HEARTBEAT_FILE``) моложе порога. Healthcheck docker compose только
помечает контейнер ``unhealthy``, а не перезапускает его: перезапуск по нему — дело
оркестратора (Kubernetes liveness) или autoheal.
"""

import argparse
import asyncio
import logging
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from types import FrameType

from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE

logger = logging.getLogger(__name__)

type Hook = Callable[[], Awaitable[None]]
type Main = Callable[[asyncio.Event], Awaitable[None]]

_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class Settings(BaseSettings):
    """Настройки запуска процесса: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        HEARTBEAT_FILE: Файл метки «жив»: цикл процесса обновляет его время изменения,
            healthcheck сверяет возраст. ``None`` — без метки (локальный запуск).
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    HEARTBEAT_FILE: Path | None = None


class Heartbeat:
    """Метка «процесс жив» — время изменения файла; без файла ничего не делает."""

    def __init__(self, path: Path | None) -> None:
        """Запоминает путь к файлу метки.

        Args:
            path: Файл метки; ``None`` — метка отключена.
        """
        self._path = path

    def __call__(self) -> None:
        """Обновляет время изменения файла метки (создаёт его при первом вызове)."""
        if self._path is not None:
            self._path.touch()

    def age(self) -> float | None:
        """Возраст метки в секундах.

        Returns:
            Сколько секунд назад процесс отметился; ``None`` — метки нет.
        """
        if self._path is None or not self._path.exists():
            return None
        return max(0.0, time.time() - self._path.stat().st_mtime)


def run(main: Main, *, setup: Sequence[Hook] = (), cleanup: Sequence[Hook] = ()) -> None:
    """Выполняет процесс до остановки по сигналу; исключение — код выхода 1.

    Args:
        main: Цикл процесса; получает событие остановки и должен вернуться после него.
        setup: Хуки запуска по порядку (проверка и открытие соединений).
        cleanup: Хуки остановки по порядку; выполняются, если запуск прошёл.
    """
    try:
        asyncio.run(_serve(main, setup, cleanup))
    except Exception:
        logger.exception("Процесс остановлен из-за ошибки")
        sys.exit(1)


async def _serve(main: Main, setup: Sequence[Hook], cleanup: Sequence[Hook]) -> None:
    """Ставит обработчики сигналов, выполняет хуки запуска, цикл и хуки остановки.

    Args:
        main: Цикл процесса.
        setup: Хуки запуска.
        cleanup: Хуки остановки.
    """
    stop = asyncio.Event()
    restore = install_signal_handlers(stop)
    try:
        for hook in setup:
            await hook()
        try:
            await main(stop)
        finally:
            for hook in cleanup:
                try:
                    await hook()
                except Exception:
                    logger.warning("Хук остановки завершился с ошибкой", exc_info=True)
    finally:
        restore()


def install_signal_handlers(stop: asyncio.Event) -> Callable[[], None]:
    """SIGTERM и SIGINT ставят событие остановки; повторный сигнал отменяет цикл процесса.

    Proactor-цикл Windows не поддерживает ``add_signal_handler`` — там используется
    ``signal.signal`` с передачей в цикл через ``call_soon_threadsafe``.

    Args:
        stop: Событие остановки процесса.

    Returns:
        Функция, возвращающая прежние обработчики сигналов.
    """
    loop = asyncio.get_running_loop()

    def on_signal() -> None:
        if stop.is_set():
            raise KeyboardInterrupt
        stop.set()

    def on_signal_threadsafe(_signum: int, _frame: FrameType | None) -> None:
        if stop.is_set():
            raise KeyboardInterrupt
        loop.call_soon_threadsafe(stop.set)

    previous: dict[int, Callable[..., object] | int | None] = {}
    for signum in _SIGNALS:
        try:
            loop.add_signal_handler(signum, on_signal)
        except NotImplementedError:
            previous[signum] = signal.signal(signum, on_signal_threadsafe)

    def restore() -> None:
        for signum in _SIGNALS:
            if signum in previous:
                signal.signal(signum, previous[signum])
            else:
                loop.remove_signal_handler(signum)

    return restore


def check_heartbeat(max_age: float) -> int:
    """Код выхода healthcheck: метка «жив» есть и моложе порога.

    Args:
        max_age: Порог возраста метки в секундах.

    Returns:
        ``0`` — процесс жив; ``1`` — метки нет или она старше порога.
    """
    age = Heartbeat(Settings().HEARTBEAT_FILE).age()
    return 0 if age is not None and age < max_age else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m payment_service.launchers.process")
    parser.add_argument("--check-heartbeat", type=float, required=True, metavar="СЕКУНДЫ")
    sys.exit(check_heartbeat(parser.parse_args().check_heartbeat))
