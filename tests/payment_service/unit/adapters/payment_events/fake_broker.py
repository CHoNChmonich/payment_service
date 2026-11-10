"""Фейк брокера FastStream для unit-тестов публикатора: запоминает вызовы, отвечает по сценарию."""

from faststream.rabbit import RabbitExchange
from pamqp import commands


class FakeBroker:
    """Запоминает ``start``/``stop``/объявления и публикации; ответ публикации задаётся списком.

    Attributes:
        replies: Ответы на публикации по очереди: кадр подтверждения, ``None``, объект
            возврата или исключение; без ответа — ``Basic.Ack``.
        published: Аргументы каждой публикации.
        log: Хронология вызовов.
    """

    def __init__(self, replies: list[object] | None = None) -> None:
        self.replies = list(replies or [])
        self.published: list[dict[str, object]] = []
        self.log: list[str] = []
        self.start_error: Exception | None = None

    async def start(self) -> None:
        self.log.append("start")
        if self.start_error is not None:
            raise self.start_error

    async def stop(self) -> None:
        self.log.append("stop")

    async def declare_exchange(self, exchange: RabbitExchange) -> RabbitExchange:
        self.log.append(f"declare_exchange {exchange.name}")
        return exchange

    async def publish(self, message: bytes, **kwargs: object) -> object:
        self.published.append({"message": message, **kwargs})
        reply = self.replies.pop(0) if self.replies else commands.Basic.Ack(delivery_tag=1)
        if isinstance(reply, Exception):
            raise reply
        return reply
