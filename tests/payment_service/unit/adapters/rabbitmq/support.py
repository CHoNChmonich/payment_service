"""Общее для unit-тестов консьюмера: короткие паузы, фейки брокера и сообщения."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

from faststream.rabbit import RabbitExchange, RabbitMessage, RabbitQueue
from pamqp import commands

from payment_service.adapters import rabbitmq

type Handler = Callable[[RabbitMessage], Awaitable[None]]

TOPOLOGY = rabbitmq.QueueTopology(
    exchange="payments",
    queue="payments.new",
    routing_key="payments.new",
    dead_letter_exchange="payments.dlx",
    dead_letter_queue="payments.new.dlq",
)


def config(**overrides: float) -> rabbitmq.ConsumerConfig:
    """Настройки с короткими паузами и бюджетом — тесты повторов идут доли секунды."""
    values: dict[str, float] = {
        "handler_timeout": 0.5,
        "retry_backoff": 0.01,
        "retry_backoff_max": 0.02,
        "retry_budget": 0.2,
        "dlq_publish_timeout": 1,
        "ping_timeout": 5,
    }
    return rabbitmq.ConsumerConfig(topology=TOPOLOGY, **(values | overrides))


def error_code(error: BaseException) -> str:
    """Код причины для DLQ в тестах библиотеки: атрибут ``code`` или имя класса."""
    code = getattr(error, "code", None)
    return code if isinstance(code, str) else type(error).__name__


class FakeRaw:
    """Исходное сообщение aio-pika: тело, свойства, маршрут, подтверждения в журнал."""

    def __init__(
        self,
        body: bytes,
        log: list[str],
        *,
        message_id: str = "m-1",
        headers: dict[str, Any] | None = None,
    ) -> None:
        self.body = body
        self.log = log
        self.message_id = message_id
        self.content_type = "application/json"
        self.headers = headers or {}
        self.exchange = "payments"
        self.routing_key = "payments.new"
        self.locked = False
        self.ack_error: Exception | None = None

    async def ack(self, multiple: bool = False) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        self.log.append(f"ack {self.message_id}")

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.log.append(f"nack {self.message_id}")

    async def reject(self, requeue: bool = False) -> None:
        self.log.append(f"reject {self.message_id}")


def message(
    body: bytes, log: list[str], *, message_id: str = "m-1", headers: dict[str, Any] | None = None
) -> RabbitMessage:
    """Сообщение FastStream поверх фейка aio-pika — как его получает подписчик."""
    raw = FakeRaw(body, log, message_id=message_id, headers=headers)
    return RabbitMessage(
        cast(Any, raw),
        body,
        headers=raw.headers,
        message_id=message_id,
        content_type=raw.content_type,
    )


class FakeBroker:
    """Брокер FastStream для тестов: подписчик, доставка сообщений, публикации по сценарию.

    ``start`` отдаёт сообщения подписчику по одному в отдельной задаче и по окончании ставит
    событие остановки — как будто очередь опустела и пришёл SIGTERM.

    Attributes:
        replies: Ответы на публикации по очереди: кадр подтверждения, объект возврата или
            исключение; без ответа — ``Basic.Ack``.
        published: Аргументы каждой публикации.
        log: Хронология вызовов.
    """

    def __init__(
        self,
        messages: list[RabbitMessage],
        stop: asyncio.Event,
        log: list[str],
        *,
        replies: list[object] | None = None,
        ping_ok: bool = True,
    ) -> None:
        self.messages = list(messages)
        self.stop_event = stop
        self.log = log
        self.replies = list(replies or [])
        self.published: list[dict[str, object]] = []
        self.ping_ok = ping_ok
        self.subscription: dict[str, object] = {}
        self.handler: Handler | None = None
        self.handler_error: BaseException | None = None
        self.delivery: asyncio.Task[None] | None = None

    def subscriber(
        self, queue: RabbitQueue, exchange: RabbitExchange, **kwargs: object
    ) -> Callable[[Handler], Handler]:
        self.subscription = {
            "queue": queue.name,
            "exchange": exchange.name,
            "declare": (queue.declare, exchange.declare),
            **kwargs,
        }

        def register(handler: Handler) -> Handler:
            self.handler = handler
            return handler

        return register

    async def connect(self) -> None:
        self.log.append("connect")

    async def declare_exchange(self, exchange: RabbitExchange) -> RabbitExchange:
        self.log.append(f"declare_exchange {exchange.name}")
        return exchange

    async def declare_queue(self, queue: RabbitQueue) -> "_DeclaredQueue":
        self.log.append(f"declare_queue {queue.name}")
        return _DeclaredQueue(queue.name, self.log)

    async def start(self) -> None:
        self.log.append("start")
        self.delivery = asyncio.ensure_future(self._deliver())

    async def _deliver(self) -> None:
        # Исключение обработчика — дефект консьюмера: сохраняем, чтобы тест упал с причиной
        assert self.handler is not None
        try:
            for item in self.messages:
                await self.handler(item)
        except Exception as exc:
            self.handler_error = exc
        finally:
            self.stop_event.set()

    async def ping(self, timeout: float) -> bool:
        self.log.append("ping")
        return self.ping_ok

    async def stop(self) -> None:
        self.log.append("stop")
        if self.delivery is not None:
            self.delivery.cancel()

    async def publish(self, message: bytes, **kwargs: object) -> object:
        self.published.append({"message": message, **kwargs})
        reply = self.replies.pop(0) if self.replies else commands.Basic.Ack(delivery_tag=1)
        if isinstance(reply, Exception):
            raise reply
        return reply


class _DeclaredQueue:
    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    async def bind(self, exchange: RabbitExchange, routing_key: str) -> None:
        self.log.append(f"bind {self.name} -> {exchange.name} [{routing_key}]")
