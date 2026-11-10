"""Сценарий доставки событий платежей из outbox."""

from payment_service.application import interfaces


class EventRelay(interfaces.EventRelay):
    """Доставка событий outbox; каждый вызов ``relay`` — отдельная транзакция.

    Пачка закрепляется за транзакцией, публикуется и удаляется в ней же: сбой публикации
    откатывает её, события остаются и уйдут при следующем вызове (доставка не менее одного
    раза). Транзакция держится открытой на время ожидания брокера — его ограничивает таймаут
    публикации адаптера и размер пачки.

    Attributes:
        uow_factory: Вызываемый объект, создающий новый Unit of Work на каждый вызов.
        publisher: Публикация событий в очередь обработки.
        batch_size: Наибольшее число событий за один вызов.
    """

    def __init__(
        self,
        uow_factory: interfaces.UnitOfWorkFactory,
        publisher: interfaces.EventPublisher,
        batch_size: int,
    ) -> None:
        """Принимает зависимости из composition root.

        Args:
            uow_factory: Фабрика Unit of Work — вызываемый объект без аргументов.
            publisher: Публикация событий.
            batch_size: Наибольшее число событий за один вызов.
        """
        self.uow_factory = uow_factory
        self.publisher = publisher
        self.batch_size = batch_size

    async def relay(self) -> int:
        """Публикует пачку закреплённых за транзакцией событий и удаляет их.

        Returns:
            Сколько событий опубликовано и удалено.
        """
        async with self.uow_factory() as uow:
            events = await uow.outbox.fetch_batch(self.batch_size)
            if not events:
                return 0
            await self.publisher.publish(events)
            await uow.outbox.delete([event.event_id for event in events])
            await uow.commit()
            return len(events)
