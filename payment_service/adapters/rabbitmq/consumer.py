"""Цикл консьюмера очереди: подписка, обработка, повторы, DLQ, подтверждения.

Роль, которую у HTTP играет сервер: доставляет сообщения обработчику адаптера и превращает исход
в действие с сообщением по политике адаптера. Доставка не менее одного раза: сообщение
подтверждается (``ack``) только после успеха или подтверждённой записи в DLQ; повторную
доставку гасит обработчик (inbox сценария). Что делать с исключением, решает политика адаптера
(``Action``): библиотека не знает ни ошибок ядра, ни формата сообщений.
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from .dead_letters import DeadLetters, DeadLetterTarget, DeadLetterUnavailable
from .topology import QueueTopology, declare

logger = logging.getLogger(__name__)

# Как часто цикл консьюмера проверяет соединение с брокером и ставит метку «жив».
PING_INTERVAL = 5.0
# Запас ``graceful_timeout`` брокера сверх попытки и записи в DLQ: подтверждение и закрытие канала.
GRACEFUL_MARGIN = 5.0
# Запас остановки процесса сверх ``graceful_timeout``: закрытие соединения, хуки остановки.
CLOSE_MARGIN = 10.0

type Handler = Callable[[bytes], Awaitable[bool]]
"""Обработчик тела: ``True`` — эффект применён сейчас, ``False`` — сообщение уже обработано."""


class Action(StrEnum):
    """Что делать с сообщением после неудачной попытки обработки."""

    DEAD_LETTER = "dead_letter"
    """Сообщение не обработать как есть: в DLQ, подтвердить, обработка идёт дальше."""

    RETRY = "retry"
    """Временный сбой собственной инфраструктуры: повтор того же сообщения после паузы."""

    STOP = "stop"
    """Дефект кода или конфигурации: остановить процесс, сообщение не подтверждать."""


type Policy = Callable[[BaseException], Action]
"""Политика адаптера: исключение попытки → действие консьюмера."""


@dataclass(frozen=True)
class ConsumerConfig:
    """Очередь, дедлайны и повторы консьюмера; строит адаптер из своих настроек.

    Attributes:
        topology: Рабочая очередь, обменник, DLX и DLQ.
        handler_timeout: Дедлайн одной попытки обработки в секундах; превышение — повтор.
        retry_backoff: Пауза перед первым повтором; дальше удваивается.
        retry_backoff_max: Наибольшая пауза между повторами.
        retry_budget: Сколько секунд одно сообщение может повторяться; по исчерпании процесс
            останавливается без подтверждения.
        dlq_publish_timeout: Дедлайн подтверждения записи в DLQ в секундах.
        ping_timeout: Сколько секунд ждать соединения с брокером на каждой проверке цикла.
    """

    topology: QueueTopology
    handler_timeout: float
    retry_backoff: float
    retry_backoff_max: float
    retry_budget: float
    dlq_publish_timeout: float
    ping_timeout: float

    @property
    def dead_letter_target(self) -> DeadLetterTarget:
        """DLQ консьюмера и дедлайн записи в неё."""
        return DeadLetterTarget(topology=self.topology, publish_timeout=self.dlq_publish_timeout)

    @property
    def graceful_timeout(self) -> float:
        """Сколько секунд брокер ждёт завершения текущего сообщения при остановке.

        Попытка обработки, запись в DLQ и запас на подтверждение: сообщение, обработка которого
        началась до сигнала, дорабатывает и подтверждается.
        """
        return self.handler_timeout + self.dlq_publish_timeout + GRACEFUL_MARGIN

    @property
    def stop_grace(self) -> float:
        """Сколько секунд нужно на остановку; ``stop_grace_period`` контейнера — не меньше."""
        return self.graceful_timeout + CLOSE_MARGIN

    @property
    def heartbeat_gap(self) -> float:
        """Наибольший промежуток между метками «жив» в секундах; порог healthcheck не меньше.

        Цикл консьюмера ставит метку после каждой успешной проверки соединения, независимо от
        обработки сообщений: промежуток — интервал проверки и её дедлайн.
        """
        return PING_INTERVAL + self.ping_timeout


class RetryBudgetExhausted(RuntimeError):
    """Временный сбой не прошёл за бюджет повторов — процесс останавливается без подтверждения."""


class QueueConsumer:
    """Консьюмер одной очереди; ``run`` — цикл процесса до события остановки.

    Политика исхода попытки — внедрённая ``action_for``: ``DEAD_LETTER`` — в DLQ и подтвердить,
    ``RETRY`` — повтор с удваивающейся паузой в пределах бюджета (иначе остановка), ``STOP`` —
    исключение сохраняется, процесс останавливается без подтверждения. FastStream глотает
    исключения подписчика, поэтому остановка идёт через событие ``failed``: цикл ``run``
    замечает его, останавливает брокер и перебрасывает сохранённое исключение в
    ``process.run``. Сигнал остановки прерывает паузу между повторами: сообщение остаётся
    неподтверждённым и вернётся в очередь при закрытии канала.
    """

    def __init__(
        self,
        handler: Handler,
        action_for: Policy,
        dead_letters: DeadLetters,
        config: ConsumerConfig,
        broker: RabbitBroker,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> None:
        """Принимает обработчик и политику адаптера, DLQ, настройки и брокер из composition root.

        Args:
            handler: Обработчик тела сообщения.
            action_for: Политика ошибок адаптера.
            dead_letters: Публикатор DLQ на том же брокере.
            config: Очередь, дедлайны и повторы.
            broker: Брокер FastStream; подключается и останавливается в ``run``.
            heartbeat: Отметка «процесс жив»: после каждой проверки соединения и перед каждой
                попыткой, паузой и записью в DLQ.
        """
        self._handler = handler
        self._action_for = action_for
        self._dead_letters = dead_letters
        self._config = config
        self._broker = broker
        self._heartbeat = heartbeat
        self._queue = config.topology.queue
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
        topology = self._config.topology
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
                if await self._broker.ping(self._config.ping_timeout):
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
        retries = _Retries(self._config, self._stop)
        while True:
            self._heartbeat()
            retries.begin_attempt()
            try:
                async with asyncio.timeout(self._config.handler_timeout):
                    applied = await self._handler(message.raw_message.body)
            except Exception as exc:
                action = self._action_for(exc)
                if action is Action.STOP:
                    self._fail(exc)
                    return
                if action is Action.DEAD_LETTER:
                    await self._dead_letter(message, exc, retries)
                    return
                logger.warning(
                    "Временный сбой обработки сообщения из %s, попытка %s",
                    self._queue,
                    retries.attempt,
                    exc_info=True,
                )
                self._heartbeat()
                if not await self._pause(retries, exc):
                    return
                continue
            if applied:
                logger.info("Сообщение из %s обработано", self._queue)
            else:
                logger.info("Сообщение из %s обработано ранее — повторная доставка", self._queue)
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
                "Сообщение из %s отправлено в DLQ: %s",
                self._queue,
                error,
                exc_info=(type(error), error, None),
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
        logger.error("Консьюмер %s останавливается: %s", self._queue, error, exc_info=error)
        self._failure = error
        self._failed.set()

    @staticmethod
    async def _ack(message: RabbitMessage) -> None:
        """Подтверждает сообщение; отказ канала — предупреждение, сообщение вернётся само.

        Обычная причина отказа — канал закрыт после переподключения: брокер уже вернул
        неподтверждённое сообщение в очередь, повтор погасит обработчик.

        Args:
            message: Обработанное сообщение.
        """
        try:
            await message.ack()
        except Exception:
            logger.warning("Подтверждение не отправлено (канал закрыт?)", exc_info=True)


class _Retries:
    """Попытки одного сообщения: счётчик, пауза с удвоением и потолком, бюджет, остановка."""

    def __init__(self, config: ConsumerConfig, stop: asyncio.Event) -> None:
        """Отсчитывает бюджет повторов от первой попытки.

        Args:
            config: Пауза, её потолок и бюджет повторов.
            stop: Событие остановки процесса — прерывает паузу.
        """
        self._loop = asyncio.get_running_loop()
        self._deadline = self._loop.time() + config.retry_budget
        self._backoff = config.retry_backoff
        self._backoff_max = config.retry_backoff_max
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
