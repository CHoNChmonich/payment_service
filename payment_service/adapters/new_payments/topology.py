"""Топология консьюмера: рабочая очередь с обменником и Dead Letter Queue."""

from dataclasses import dataclass

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.adapters import rabbitmq


@dataclass(frozen=True)
class QueueTopology:
    """Очередь с обменником и очередью недоставленных сообщений; имена — из настроек адаптера.

    Attributes:
        exchange: Обменник, из которого очередь получает сообщения (direct).
        queue: Рабочая очередь.
        routing_key: Ключ привязки очереди к обменнику.
        dead_letter_exchange: Обменник недоставленных сообщений (direct).
        dead_letter_queue: Очередь недоставленных сообщений; она же ключ привязки к DLX.
    """

    exchange: str
    queue: str
    routing_key: str
    dead_letter_exchange: str
    dead_letter_queue: str

    def exchange_object(self, *, declare: bool = True) -> RabbitExchange:
        """Рабочий обменник: direct, durable.

        Args:
            declare: Объявлять ли обменник при использовании (``False`` — он уже объявлен).

        Returns:
            Описание обменника для FastStream.
        """
        return rabbitmq.direct_exchange(self.exchange, declare=declare)

    def queue_object(self, *, declare: bool = True) -> RabbitQueue:
        """Рабочая очередь: durable, с переадресацией отвергнутых сообщений в DLX.

        Аргументы ``x-dead-letter-*`` нельзя изменить у существующей очереди: другое значение
        при объявлении даёт ``PRECONDITION_FAILED``, очередь нужно удалить или переименовать.

        Args:
            declare: Объявлять ли очередь при использовании (``False`` — она уже объявлена).

        Returns:
            Описание очереди для FastStream.
        """
        return RabbitQueue(
            self.queue,
            durable=True,
            routing_key=self.routing_key,
            declare=declare,
            arguments={
                "x-dead-letter-exchange": self.dead_letter_exchange,
                "x-dead-letter-routing-key": self.dead_letter_queue,
            },
        )

    def dead_letter_exchange_object(self, *, declare: bool = True) -> RabbitExchange:
        """Обменник недоставленных сообщений: direct, durable.

        Args:
            declare: Объявлять ли обменник при использовании (``False`` — он уже объявлен).

        Returns:
            Описание обменника для FastStream.
        """
        return rabbitmq.direct_exchange(self.dead_letter_exchange, declare=declare)

    def dead_letter_queue_object(self) -> RabbitQueue:
        """Очередь недоставленных сообщений: durable, привязана к DLX своим именем.

        Returns:
            Описание очереди для FastStream.
        """
        return RabbitQueue(self.dead_letter_queue, durable=True, routing_key=self.dead_letter_queue)


async def declare(broker: RabbitBroker, topology: QueueTopology) -> None:
    """Объявляет обменники, очереди и привязки; повторное объявление идемпотентно.

    Единственное место объявления топологии консьюмера: подписчик получает описания с
    ``declare=False``. Порядок: сначала DLX и DLQ, затем рабочий обменник и очередь, чтобы к
    моменту первого отвергнутого сообщения было куда его переложить.

    Args:
        broker: Подключённый брокер.
        topology: Имена объектов.
    """
    dlx = await broker.declare_exchange(topology.dead_letter_exchange_object())
    dlq = await broker.declare_queue(topology.dead_letter_queue_object())
    await dlq.bind(dlx, routing_key=topology.dead_letter_queue)
    exchange = await broker.declare_exchange(topology.exchange_object())
    queue = await broker.declare_queue(topology.queue_object())
    await queue.bind(exchange, routing_key=topology.routing_key)
