"""Опрос outbox по таймеру — входящий адаптер сценария доставки событий."""

import asyncio
import contextlib
import logging
from collections.abc import Callable

from payment_service.application import errors, interfaces

from .settings import Settings

logger = logging.getLogger(__name__)


class OutboxPoller:
    """Вызывает сценарий доставки, пока есть что публиковать; без работы — пауза.

    Входящий адаптер, как контроллер у HTTP, но источник вызова — таймер: зависит от порта
    ``interfaces.EventRelay``, а не от сервиса. ``UnavailableError`` (брокер или БД
    недоступны) — ожидаемый временный отказ: предупреждение и пауза. Любое другое исключение
    уходит наружу и останавливает процесс: оркестратор перезапустит его, а сбой будет виден.

    Attributes:
        relay: Сценарий доставки событий outbox.
    """

    def __init__(
        self,
        relay: interfaces.EventRelay,
        settings: Settings,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> None:
        """Принимает порт сценария и настройки из composition root.

        Args:
            relay: Сценарий доставки событий outbox.
            settings: Пауза после пустого вызова.
            heartbeat: Отметка «процесс жив», вызывается на каждой итерации.
        """
        self.relay = relay
        self._idle_interval = settings.OUTBOX_POLLER_IDLE_INTERVAL
        self._heartbeat = heartbeat

    async def run(self, stop: asyncio.Event) -> None:
        """Цикл опроса до события остановки; текущий вызов доставки дорабатывает до конца.

        Args:
            stop: Событие остановки процесса (SIGTERM).
        """
        while not stop.is_set():
            self._heartbeat()
            try:
                published = await self.relay.relay()
            except errors.UnavailableError:
                logger.warning("Доставка событий отложена: зависимость недоступна", exc_info=True)
                published = 0
            if not published:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._idle_interval)
