"""Сценарий доставки событий платежей из outbox."""

from payment_service.application import dtos, errors, interfaces


class EventRelay(interfaces.EventRelay):
    """Доставка событий outbox; у каждого типа события — своя дорожка и своя транзакция.

    Пачка типа закрепляется за транзакцией, публикуется и удаляется в ней же: сбой публикации
    откатывает её, события остаются и уйдут при следующем вызове (доставка не менее одного
    раза). Дорожки независимы: пока публикация одного типа не подтверждается, события
    остальных типов доставляются. Транзакция держится открытой на время ожидания публикации —
    его ограничивает таймаут публикатора и размер пачки.

    Attributes:
        uow_factory: Вызываемый объект, создающий новый Unit of Work на каждый вызов.
        publisher: Публикация событий в очередь обработки.
        batch_size: Наибольшее число событий одного типа за один вызов.
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
            batch_size: Наибольшее число событий одного типа за один вызов.
        """
        self.uow_factory = uow_factory
        self.publisher = publisher
        self.batch_size = batch_size

    async def relay(self) -> dtos.RelayResult:
        """Проходит дорожки всех типов событий; первый сбой — наружу, если не вышло ничего.

        Временный сбой одной дорожки при успехе другой не поднимается, а попадает в итог:
        вызывающий сообщит о нём и повторит вызов сразу.

        Returns:
            Число опубликованных и удалённых событий и типы дорожек со сбоем.
        """
        published = 0
        unavailable: list[dtos.PaymentEventType] = []
        failure: errors.UnavailableError | None = None
        for event_type in dtos.PAYMENT_EVENT_TYPES:
            try:
                published += await self._relay_lane(event_type)
            except errors.UnavailableError as exc:
                failure = failure or exc
                unavailable.append(event_type)
        if failure is not None and not published:
            raise failure
        return dtos.RelayResult(published=published, unavailable=tuple(unavailable))

    async def _relay_lane(self, event_type: dtos.PaymentEventType) -> int:
        """Публикует пачку событий одного типа и удаляет её в одной транзакции.

        Args:
            event_type: Тип событий дорожки.

        Returns:
            Сколько событий опубликовано и удалено.
        """
        async with self.uow_factory() as uow:
            events = await uow.outbox.fetch_batch(self.batch_size, event_type)
            if not events:
                return 0
            await self.publisher.publish(events)
            await uow.outbox.delete([event.event_id for event in events])
            await uow.commit()
            return len(events)
