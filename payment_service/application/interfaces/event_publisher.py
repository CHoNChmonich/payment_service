"""Исходящий порт публикации событий платежей в очередь обработки."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from payment_service.application import dtos


class EventPublisher(ABC):
    """Публикация событий платежей для консьюмера (брокер сообщений)."""

    @abstractmethod
    async def publish(self, events: Sequence[dtos.PaymentCreated]) -> None:
        """Публикует события и ждёт подтверждения доставки каждого.

        Доставка — не менее одного раза: после сбоя часть событий могла дойти, и повтор
        опубликует их снова; консьюмер отбрасывает дубли по состоянию платежа.

        Args:
            events: События в порядке публикации.

        Raises:
            BrokerUnavailable: Временный сбой — доставка не подтверждена; повтор допустим.
        """
