from unittest.mock import AsyncMock

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

from payment_service.adapters import rabbitmq

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
TOPOLOGY = rabbitmq.QueueTopology(
    exchange="payments",
    queue="payments.new",
    routing_key="payments.new",
    dead_letter_exchange="payments.dlx",
    dead_letter_queue="payments.new.dlq",
)


class RecordingBroker:
    """Фейк брокера: запоминает объявления и привязки по порядку."""

    def __init__(self) -> None:
        self.log: list[str] = []

    async def declare_exchange(self, exchange: RabbitExchange) -> RabbitExchange:
        self.log.append(f"exchange {exchange.name}")
        return exchange

    async def declare_queue(self, queue: RabbitQueue) -> AsyncMock:
        self.log.append(f"queue {queue.name}")
        declared = AsyncMock()
        declared.bind.side_effect = lambda exchange, routing_key: self.log.append(
            f"bind {queue.name} -> {exchange.name} [{routing_key}]"
        )
        return declared


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestQueueTopology:
    def test__objects_without_declaration(self) -> None:
        # Call
        queue = TOPOLOGY.queue_object(declare=False)
        exchange = TOPOLOGY.exchange_object(declare=False)
        dlx = TOPOLOGY.dead_letter_exchange_object(declare=False)

        # Assert: описания для подписчика и публикатора DLQ — без повторного объявления
        assert (queue.declare, exchange.declare, dlx.declare) == (False, False, False)
        assert TOPOLOGY.queue_object().declare is True

    def test__objects(self) -> None:
        # Call
        exchange = TOPOLOGY.exchange_object()
        queue = TOPOLOGY.queue_object()
        dlx = TOPOLOGY.dead_letter_exchange_object()
        dlq = TOPOLOGY.dead_letter_queue_object()

        # Assert: durable direct-обменники; рабочая очередь переадресует в DLX по имени DLQ
        assert (exchange.name, exchange.type, exchange.durable) == (
            "payments",
            ExchangeType.DIRECT,
            True,
        )
        assert (dlx.name, dlx.type, dlx.durable) == ("payments.dlx", ExchangeType.DIRECT, True)
        assert (queue.name, queue.durable, queue.routing_key) == (
            "payments.new",
            True,
            "payments.new",
        )
        arguments = dict(queue.arguments or {})
        assert arguments.pop("x-queue-type", "classic") == "classic"  # добавляет FastStream
        assert arguments == {
            "x-dead-letter-exchange": "payments.dlx",
            "x-dead-letter-routing-key": "payments.new.dlq",
        }
        assert (dlq.name, dlq.durable, dlq.routing_key) == (
            "payments.new.dlq",
            True,
            "payments.new.dlq",
        )
        assert "x-dead-letter-exchange" not in (dlq.arguments or {})


class TestDeclare:
    async def test__declares_dlq_first_and_binds(self) -> None:
        # Setup
        broker = RecordingBroker()

        # Call
        await rabbitmq.declare(broker, TOPOLOGY)  # type: ignore[arg-type]

        # Assert
        assert broker.log == [
            "exchange payments.dlx",
            "queue payments.new.dlq",
            "bind payments.new.dlq -> payments.dlx [payments.new.dlq]",
            "exchange payments",
            "queue payments.new",
            "bind payments.new -> payments [payments.new]",
        ]
