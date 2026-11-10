"""Исходящий порт публикации событий платежей в очередь обработки."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from payment_service.application import dtos


class EventPublisher(ABC):
    """Публикация событий платежей в очереди обработки (брокер сообщений)."""

    @abstractmethod
    async def publish(self, events: Sequence[dtos.AnyPaymentEvent]) -> None:
        """Публикует события и ждёт подтверждения доставки каждого.

        Доставка — не менее одного раза: после сбоя часть событий могла дойти, и повтор
        опубликует их снова; потребители отбрасывают дубли по ``event_id``.

        Args:
            events: События в порядке публикации.

        Raises:
            BrokerUnavailable: Временный сбой — доставка не подтверждена; повтор допустим.
        """
