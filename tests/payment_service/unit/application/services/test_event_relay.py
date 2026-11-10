from collections.abc import Sequence
from unittest.mock import MagicMock, Mock, call
from uuid import UUID

import pytest

from payment_service.application import dtos, errors, interfaces, services


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def uow() -> MagicMock:
    unit_of_work = MagicMock(spec=interfaces.UnitOfWork)
    unit_of_work.__aenter__.return_value = unit_of_work
    unit_of_work.outbox = Mock(interfaces.OutboxRepo)
    return unit_of_work


@pytest.fixture(scope="function")
def publisher() -> Mock:
    return Mock(interfaces.EventPublisher)


@pytest.fixture(scope="function")
def service(uow: MagicMock, publisher: Mock) -> services.EventRelay:
    return services.EventRelay(
        uow_factory=Mock(return_value=uow), publisher=publisher, batch_size=10
    )


CREATED = [
    dtos.PaymentCreated(payment_id=UUID("00000000-0000-0000-0000-000000000001")),
    dtos.PaymentCreated(payment_id=UUID("00000000-0000-0000-0000-000000000002")),
]
PROCESSED = [dtos.PaymentProcessed(payment_id=UUID("00000000-0000-0000-0000-000000000003"))]


def _outbox(
    created: Sequence[dtos.AnyPaymentEvent], processed: Sequence[dtos.AnyPaymentEvent]
) -> Mock:
    """Подмена ``fetch_batch``: пачка по типу события."""
    lanes = {"payment_created": created, "payment_processed": processed}
    return Mock(side_effect=lambda limit, event_type: lanes[event_type])


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRelay:
    async def test__relay(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.side_effect = _outbox(CREATED, PROCESSED)
        manager = Mock()
        manager.attach_mock(uow, "uow")
        manager.attach_mock(publisher, "publisher")

        # Call
        result = await service.relay()

        # Assert
        assert manager.mock_calls == [
            # Дорожка каждого типа — своя транзакция: пачки типов не смешиваются
            call.uow.__aenter__(),
            call.uow.outbox.fetch_batch(10, "payment_created"),
            call.publisher.publish(CREATED),  # удаление — только после подтверждения брокера
            call.uow.outbox.delete([CREATED[0].event_id, CREATED[1].event_id]),
            call.uow.commit(),
            call.uow.__aexit__(None, None, None),
            call.uow.__aenter__(),
            call.uow.outbox.fetch_batch(10, "payment_processed"),
            call.publisher.publish(PROCESSED),
            call.uow.outbox.delete([PROCESSED[0].event_id]),
            call.uow.commit(),
            call.uow.__aexit__(None, None, None),
        ]
        assert result == dtos.RelayResult(published=3)

    async def test__empty(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.side_effect = _outbox([], [])

        # Call
        result = await service.relay()

        # Assert
        assert result == dtos.RelayResult()
        assert uow.outbox.fetch_batch.call_count == len(dtos.PAYMENT_EVENT_TYPES)
        publisher.publish.assert_not_called()
        uow.commit.assert_not_called()

    async def test__broker_unavailable(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.side_effect = _outbox(CREATED, PROCESSED)
        publisher.publish.side_effect = errors.BrokerUnavailable()

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable):
            await service.relay()

        assert publisher.publish.call_count == 2  # сбой дорожки не останавливает остальные
        uow.outbox.delete.assert_not_called()  # события остаются в outbox
        uow.commit.assert_not_called()

    async def test__one_lane_unavailable(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.side_effect = _outbox(CREATED, PROCESSED)

        async def publish(events: Sequence[dtos.AnyPaymentEvent]) -> None:
            if events[0].type == "payment_created":
                raise errors.BrokerUnavailable()

        publisher.publish.side_effect = publish

        # Call
        result = await service.relay()

        # Assert: здоровая дорожка доставлена, сбой не поднят — вызывающий повторит сразу
        assert result == dtos.RelayResult(
            published=len(PROCESSED), unavailable=("payment_created",)
        )
        uow.outbox.delete.assert_called_once_with([PROCESSED[0].event_id])
        uow.commit.assert_called_once()

    async def test__failed_lane_raises_once_others_are_empty(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup: у здоровой дорожки событий нет
        uow.outbox.fetch_batch.side_effect = _outbox(CREATED, [])
        failure = errors.BrokerUnavailable()
        publisher.publish.side_effect = failure

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable) as exc_info:
            await service.relay()

        assert exc_info.value is failure
        uow.commit.assert_not_called()

    async def test__defect_stops_relay(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.side_effect = _outbox(CREATED, PROCESSED)
        publisher.publish.side_effect = RuntimeError("дефект")

        # Call and Assert
        with pytest.raises(RuntimeError):
            await service.relay()

        publisher.publish.assert_called_once()  # не временный сбой — дорожки дальше не идут
        uow.commit.assert_not_called()
