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


EVENTS = [
    dtos.PaymentCreated(payment_id=UUID("00000000-0000-0000-0000-000000000001")),
    dtos.PaymentCreated(payment_id=UUID("00000000-0000-0000-0000-000000000002")),
]


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRelay:
    async def test__relay(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.return_value = EVENTS
        manager = Mock()
        manager.attach_mock(uow, "uow")
        manager.attach_mock(publisher, "publisher")

        # Call
        result = await service.relay()

        # Assert
        assert manager.mock_calls == [
            call.uow.__aenter__(),
            call.uow.outbox.fetch_batch(10),
            call.publisher.publish(EVENTS),  # удаление — только после подтверждения брокера
            call.uow.outbox.delete([EVENTS[0].event_id, EVENTS[1].event_id]),
            call.uow.commit(),
            call.uow.__aexit__(None, None, None),
        ]
        assert result == 2

    async def test__empty(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.return_value = []

        # Call
        result = await service.relay()

        # Assert
        assert result == 0
        publisher.publish.assert_not_called()
        uow.commit.assert_not_called()

    async def test__broker_unavailable(
        self, service: services.EventRelay, uow: MagicMock, publisher: Mock
    ) -> None:
        # Setup
        uow.outbox.fetch_batch.return_value = EVENTS
        publisher.publish.side_effect = errors.BrokerUnavailable()

        # Call and Assert
        with pytest.raises(errors.BrokerUnavailable):
            await service.relay()

        uow.outbox.delete.assert_not_called()  # события остаются в outbox
        uow.commit.assert_not_called()
