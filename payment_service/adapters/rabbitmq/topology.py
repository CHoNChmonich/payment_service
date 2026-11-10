"""Описания объектов RabbitMQ, общие для публикатора и консьюмера."""

from faststream.rabbit import ExchangeType, RabbitExchange


def direct_exchange(name: str, *, declare: bool = True) -> RabbitExchange:
    """Описание durable-обменника типа direct.

    Args:
        name: Имя обменника.
        declare: Объявлять ли обменник при использовании (``False`` — он уже объявлен).

    Returns:
        Описание обменника для FastStream.
    """
    return RabbitExchange(name, type=ExchangeType.DIRECT, durable=True, declare=declare)
