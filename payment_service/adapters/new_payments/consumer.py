"""Цикл консьюмера новых платежей: подписка, обработка, повторы, DLQ, подтверждения.

Роль, которую у HTTP играет сервер: доставляет сообщения обработчику и превращает исход в
действие с сообщением. Доставка не менее одного раза: сообщение подтверждается (``ack``) только
после успеха или подтверждённой записи в DLQ; повторную доставку гасит состояние платежа.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable, Iterable

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from .dead_letters import DeadLetters, DeadLetterUnavailable
from .handler import NewPaymentsHandler
from .policy import Action, action_for
from .settings import PING_INTERVAL, Settings
from .topology import declare

logger = logging.getLogger(__name__)


class RetryBudgetExhausted(RuntimeError):
    """Временный сбой не прошёл за бюджет повторов — процесс останавливается без подтверждения."""


class NewPaymentsConsumer:
    """Консьюмер очереди новых платежей; ``run`` — цикл процесса до события остановки.

    Политика исхода попытки — ``policy.action_for``: ``DEAD_LETTER`` — в DLQ и подтвердить,
    ``RETRY`` — повтор с удваивающейся паузой в пределах ``NEW_PAYMENTS_RETRY_BUDGET`` (иначе
    остановка), ``STOP`` — исключение сохраняется, процесс останавливается без подтверждения.
    FastStream глотает исключения подписчика, поэтому остановка идёт через событие ``failed``:
    цикл ``run`` замечает его, останавливает брокер и перебрасывает сохранённое исключение в
    ``process.run``. Сигнал остановки прерывает паузу между повторами: сообщение остаётся
    неподтверждённым и вернётся в очередь при закрытии канала.

    Attributes:
        handler: Обработчик тела сообщения.
    """

    def __init__(
        self,
        handler: NewPaymentsHandler,
        dead_letters: DeadLetters,
        settings: Settings,
        broker: RabbitBroker,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> None:
        """Принимает обработчик, DLQ, настройки и брокер из composition root.

        Args:
            handler: Обработчик тела сообщения.
            dead_letters: Публикатор DLQ на том же брокере.
            settings: Очереди, дедлайны и повторы.
            broker: Брокер FastStream; подключается и останавливается в ``run``.
            heartbeat: Отметка «процесс жив»: после каждой проверки соединения и перед каждой
                попыткой, паузой и записью в DLQ.
        """
        self.handler = handler
        self._dead_letters = dead_letters
        self._settings = settings
        self._broker = broker
        self._heartbeat = heartbeat
        self._stop = asyncio.Event()
        self._failed = asyncio.Event()
        self._failure: BaseException | None = None

    async def run(self, stop: asyncio.Event) -> None:
        """Объявляет топологию, подписывается и работает до остановки или дефекта.

        Топологию объявляет только ``declare``; подписчик получает описания с ``declare=False``.
        Остановка брокера ждёт ``graceful_timeout`` на дорабатывающее сообщение и закрывает
        канал: неподтверждённые сообщения возвращаются в очередь; соединение закрывается и при
        ошибке объявления (``PRECONDITION_FAILED``).

        Args:
            stop: Событие остановки процесса (SIGTERM).

        Raises:
            RetryBudgetExhausted: Временный сбой длиннее бюджета повторов.
            DeadLetterFailed: Постоянный сбой записи в DLQ.
            Exception: Исключение обработки с действием ``STOP``.
        """
        self._stop = stop
        topology = self._settings.topology
        self._broker.subscriber(
            topology.queue_object(declare=False),
            topology.exchange_object(declare=False),
            ack_policy=AckPolicy.MANUAL,
        )(self._on_message)
        await self._broker.connect()
        try:
            await declare(self._broker, topology)
            await self._broker.start()
            while not stop.is_set() and not self._failed.is_set():
                if await self._broker.ping(self._settings.NEW_PAYMENTS_PING_TIMEOUT):
                    self._heartbeat()
                else:
                    logger.warning("Брокер не отвечает: метка «жив» не обновлена")
                await _wait_any((stop, self._failed), PING_INTERVAL)
        finally:
            await self._broker.stop()
        if self._failure is not None:
            raise self._failure

    async def _on_message(self, message: RabbitMessage) -> None:
        """Обрабатывает сообщение по политике ошибок; вызывается FastStream на каждое сообщение.

        Args:
            message: Сообщение очереди с исходным сообщением aio-pika внутри.
        """
        retries = _Retries(self._settings, self._stop)
        while True:
            self._heartbeat()
            retries.begin_attempt()
            try:
                async with asyncio.timeout(self._settings.NEW_PAYMENTS_HANDLER_TIMEOUT):
                    notified = await self.handler.handle(message.raw_message.body)
            except Exception as exc:
                action = action_for(exc)
                if action is Action.STOP:
                    self._fail(exc)
                    return
                if action is Action.DEAD_LETTER:
                    await self._dead_letter(message, exc, retries)
                    return
                logger.warning(
                    "Временный сбой обработки платежа, попытка %s", retries.attempt, exc_info=True
                )
                self._heartbeat()
                if not await self._pause(retries, exc):
                    return
                continue
            if notified:
                logger.info("Платёж обработан, webhook доставлен")
            else:
                logger.info("Платёж обработан ранее, webhook уже доставлен — повторная доставка")
            await self._ack(message)
            return

    async def _dead_letter(
        self, message: RabbitMessage, error: Exception, retries: "_Retries"
    ) -> None:
        """Пишет сообщение в DLQ и подтверждает его; временный сбой DLQ повторяет только запись.

        Args:
            message: Сообщение очереди.
            error: Причина отказа.
            retries: Состояние повторов этого сообщения.
        """
        while True:
            self._heartbeat()
            try:
                await self._dead_letters.publish(message, error, retries.attempt)
            except DeadLetterUnavailable as exc:
                logger.warning("DLQ недоступна, запись будет повторена", exc_info=True)
                self._heartbeat()
                if not await self._pause(retries, exc):
                    return
                continue
            except Exception as exc:
                self._fail(exc)
                return
            logger.warning(
                "Сообщение отправлено в DLQ: %s", error, exc_info=(type(error), error, None)
            )
            await self._ack(message)
            return

    async def _pause(self, retries: "_Retries", cause: Exception) -> bool:
        """Ждёт перед повтором; исчерпанный бюджет останавливает процесс.

        Args:
            retries: Состояние повторов этого сообщения.
            cause: Исключение, из-за которого нужен повтор.

        Returns:
            ``True`` — можно повторять; ``False`` — остановка (сигнал или бюджет), без ack.
        """
        try:
            return await retries.wait(cause)
        except RetryBudgetExhausted as exc:
            self._fail(exc)
            return False

    def _fail(self, error: BaseException) -> None:
        """Сохраняет дефект и даёт циклу ``run`` остановить процесс.

        Args:
            error: Исключение с действием ``STOP``.
        """
        logger.error("Консьюмер останавливается: %s", error, exc_info=error)
        self._failure = error
        self._failed.set()

    @staticmethod
    async def _ack(message: RabbitMessage) -> None:
        """Подтверждает сообщение; отказ канала — предупреждение, сообщение вернётся само.

        Обычная причина отказа — канал закрыт после переподключения: брокер уже вернул
        неподтверждённое сообщение в очередь, повтор погасит состояние платежа.

        Args:
            message: Обработанное сообщение.
        """
        try:
            await message.ack()
        except Exception:
            logger.warning("Подтверждение не отправлено (канал закрыт?)", exc_info=True)


class _Retries:
    """Попытки одного сообщения: счётчик, пауза с удвоением и потолком, бюджет, остановка."""

    def __init__(self, settings: Settings, stop: asyncio.Event) -> None:
        """Отсчитывает бюджет повторов от первой попытки.

        Args:
            settings: Пауза, её потолок и бюджет повторов.
            stop: Событие остановки процесса — прерывает паузу.
        """
        self._loop = asyncio.get_running_loop()
        self._deadline = self._loop.time() + settings.NEW_PAYMENTS_RETRY_BUDGET
        self._backoff = settings.NEW_PAYMENTS_RETRY_BACKOFF
        self._backoff_max = settings.NEW_PAYMENTS_RETRY_BACKOFF_MAX
        self._stop = stop
        self._attempt = 0

    @property
    def attempt(self) -> int:
        """Номер текущей попытки обработки, начиная с 1."""
        return self._attempt

    def begin_attempt(self) -> None:
        """Отмечает начало очередной попытки обработки."""
        self._attempt += 1

    async def wait(self, cause: Exception) -> bool:
        """Ждёт перед следующей попыткой.

        Сигнал остановки проверяется первым: остановка мягкая, даже если бюджет исчерпан.

        Args:
            cause: Исключение, из-за которого нужен повтор.

        Returns:
            ``True`` — можно повторять; ``False`` — пришёл сигнал остановки.

        Raises:
            RetryBudgetExhausted: Следующая пауза выходит за бюджет повторов.
        """
        if self._stop.is_set():
            return False
        if self._loop.time() + self._backoff > self._deadline:
            raise RetryBudgetExhausted(
                f"Временный сбой не прошёл за бюджет повторов ({self._attempt} попыток)"
            ) from cause
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), self._backoff)
        self._backoff = min(self._backoff * 2, self._backoff_max)
        return not self._stop.is_set()


async def _wait_any(events: Iterable[asyncio.Event], timeout: float) -> None:
    """Ждёт первое из событий, но не дольше ``timeout`` секунд.

    Args:
        events: События остановки.
        timeout: Предел ожидания в секундах.
    """
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()
