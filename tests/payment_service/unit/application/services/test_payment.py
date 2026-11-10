from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import ANY, MagicMock, Mock, call
from uuid import UUID

import pytest

from payment_service.application import dtos, entities, errors, interfaces, services

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
PAYMENT_ID = UUID("00000000-0000-0000-0000-000000000001")
EVENT_ID = UUID("00000000-0000-0000-0000-0000000000e1")
EVENT = dtos.PaymentEventInfo(event_id=EVENT_ID, payment_id=PAYMENT_ID)
NEW_PAYMENT_INFO = dtos.NewPaymentInfo.model_validate(
    {
        "idempotency_key": "key-1",
        "amount": Decimal("10.00"),
        "currency": entities.Currency.RUB,
        "description": "Заказ 1",
        "metadata": {"order": 1},
        "webhook_url": "http://webhook_sink:8080/hook",
    }
)


@pytest.fixture(scope="function")
def uow() -> MagicMock:
    unit_of_work = MagicMock(spec=interfaces.UnitOfWork)
    unit_of_work.__aenter__.return_value = unit_of_work
    unit_of_work.payments = Mock(interfaces.PaymentsRepo)
    unit_of_work.outbox = Mock(interfaces.OutboxRepo)
    unit_of_work.inbox = Mock(interfaces.InboxRepo)
    unit_of_work.inbox.register.return_value = True
    return unit_of_work


@pytest.fixture(scope="function")
def uow_factory(uow: MagicMock) -> Mock:
    # Фабрика — вызываемый объект без аргументов; мок каждый раз отдаёт тот же UoW
    return Mock(return_value=uow)


@pytest.fixture(scope="function")
def gateway() -> Mock:
    return Mock(interfaces.PaymentGateway)


@pytest.fixture(scope="function")
def notifier() -> Mock:
    return Mock(interfaces.WebhookNotifier)


@pytest.fixture(scope="function")
def service(uow_factory: Mock, gateway: Mock, notifier: Mock) -> services.Payment:
    return services.Payment(
        uow_factory=uow_factory, gateway=gateway, notifier=notifier, clock=lambda: NOW
    )


def _stored_payment(
    status: entities.PaymentStatus = entities.PaymentStatus.PENDING,
    *,
    notified_at: datetime | None = None,
) -> entities.Payment:
    return entities.Payment(
        id=PAYMENT_ID,
        idempotency_key="key-1",
        amount=Decimal("10.00"),
        currency=entities.Currency.RUB,
        description="Заказ 1",
        metadata={"order": 1},
        webhook_url="http://webhook_sink:8080/hook",
        status=status,
        created_at=NOW,
        processed_at=None if status is entities.PaymentStatus.PENDING else NOW,
        notified_at=notified_at,
    )


def _added_payment(uow: MagicMock) -> entities.Payment:
    """Единственный платёж, переданный репозиторию на вставку."""
    uow.payments.add.assert_called_once()
    payment: entities.Payment = uow.payments.add.call_args.args[0]
    return payment


def _added_event(uow: MagicMock) -> dtos.AnyPaymentEvent:
    """Единственное событие, добавленное в outbox."""
    uow.outbox.add.assert_called_once()
    event: dtos.AnyPaymentEvent = uow.outbox.add.call_args.args[0]
    return event


def _fingerprint(uow: MagicMock) -> str:
    """Отпечаток единственной регистрации события в inbox."""
    uow.inbox.register.assert_called_once()
    fingerprint: str = uow.inbox.register.call_args.args[1]
    return fingerprint


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCreate:
    async def test__create(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.payments.add.return_value = True

        # Call
        result = await service.create(NEW_PAYMENT_INFO)

        # Assert
        assert uow.mock_calls == [
            call.__aenter__(),
            call.payments.add(ANY),
            call.outbox.add(ANY),  # событие — в той же транзакции, до commit
            call.commit(),
            call.__aexit__(None, None, None),
        ]
        payment = _added_payment(uow)
        assert payment.status is entities.PaymentStatus.PENDING
        assert payment.created_at == NOW
        assert payment.webhook_url == "http://webhook_sink:8080/hook"
        event = _added_event(uow)
        assert isinstance(event, dtos.PaymentCreated)
        assert (event.payment_id, event.occurred_at) == (payment.id, NOW)
        assert result == dtos.PaymentInfo(
            id=payment.id,
            idempotency_key="key-1",
            amount=Decimal("10.00"),
            currency=entities.Currency.RUB,
            description="Заказ 1",
            metadata={"order": 1},
            webhook_url="http://webhook_sink:8080/hook",
            status=entities.PaymentStatus.PENDING,
            created_at=NOW,
            processed_at=None,
            notified_at=None,
        )

    async def test__same_request_repeated(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        existing = _stored_payment(entities.PaymentStatus.SUCCEEDED)
        uow.payments.add.return_value = False
        uow.payments.fetch_by_idempotency_key.return_value = existing

        # Call
        result = await service.create(NEW_PAYMENT_INFO)

        # Assert
        assert uow.mock_calls == [
            call.__aenter__(),
            call.payments.add(ANY),
            call.payments.fetch_by_idempotency_key("key-1"),
            call.__aexit__(None, None, None),
        ]
        uow.outbox.add.assert_not_called()  # повтор не порождает второй обработки
        uow.commit.assert_not_called()
        assert result.id == PAYMENT_ID
        assert result.status is entities.PaymentStatus.SUCCEEDED

    async def test__conflicting_request(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.payments.add.return_value = False
        uow.payments.fetch_by_idempotency_key.return_value = _stored_payment()

        # Call and Assert
        with pytest.raises(errors.IdempotencyKeyConflict) as exc_info:
            await service.create(NEW_PAYMENT_INFO.model_copy(update={"amount": Decimal("11.00")}))

        assert exc_info.value.context == {"idempotency_key": "key-1"}
        uow.outbox.add.assert_not_called()
        uow.commit.assert_not_called()

    async def test__row_vanished(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.payments.add.return_value = False
        uow.payments.fetch_by_idempotency_key.return_value = None

        # Call and Assert
        with pytest.raises(errors.StorageUnavailable):
            await service.create(NEW_PAYMENT_INFO)

        uow.commit.assert_not_called()


class TestGet:
    async def test__get(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = _stored_payment()

        # Call
        result = await service.get(PAYMENT_ID)

        # Assert
        assert uow.mock_calls == [
            call.__aenter__(),
            call.payments.fetch_by_id(PAYMENT_ID),
            call.__aexit__(None, None, None),
        ]
        assert result.id == PAYMENT_ID

    async def test__not_found(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = None

        # Call and Assert
        with pytest.raises(errors.PaymentNotFound) as exc_info:
            await service.get(PAYMENT_ID)

        assert exc_info.value.context == {"id": PAYMENT_ID}


class TestProcess:
    @pytest.mark.parametrize(
        ("gateway_error", "status"),
        [
            (None, entities.PaymentStatus.SUCCEEDED),
            (errors.GatewayUnavailable(), entities.PaymentStatus.FAILED),
        ],
        ids=["succeeded", "gateway_refused"],
    )
    async def test__processed(
        self,
        service: services.Payment,
        uow: MagicMock,
        gateway: Mock,
        gateway_error: errors.Error | None,
        status: entities.PaymentStatus,
    ) -> None:
        # Setup
        payment = _stored_payment()
        uow.payments.fetch_by_id.return_value = payment
        gateway.charge.side_effect = gateway_error
        manager = Mock()
        manager.attach_mock(uow, "uow")
        manager.attach_mock(gateway, "gateway")

        # Call
        result = await service.process(EVENT)

        # Assert
        assert manager.mock_calls == [
            call.uow.__aenter__(),
            call.uow.inbox.register(EVENT_ID, ANY),  # первой операцией транзакции
            call.uow.payments.fetch_by_id(PAYMENT_ID, for_update=True),
            call.gateway.charge(ANY),  # под блокировкой, до фиксации результата
            call.uow.outbox.add(ANY),  # событие — в той же транзакции, до commit
            call.uow.commit(),
            call.uow.__aexit__(None, None, None),
        ]
        charged: dtos.PaymentInfo = gateway.charge.call_args.args[0]
        assert (charged.id, charged.status) == (PAYMENT_ID, entities.PaymentStatus.PENDING)
        # Отказ шлюза — результат платежа, а не сбой: тоже фиксируется и порождает событие
        assert (payment.status, payment.processed_at) == (status, NOW)
        event = _added_event(uow)
        assert isinstance(event, dtos.PaymentProcessed)
        assert (event.payment_id, event.occurred_at) == (PAYMENT_ID, NOW)
        assert result is True

    async def test__duplicate_event(
        self, service: services.Payment, uow: MagicMock, gateway: Mock
    ) -> None:
        # Setup
        uow.inbox.register.return_value = False

        # Call
        result = await service.process(EVENT)

        # Assert
        assert uow.mock_calls == [
            call.__aenter__(),
            call.inbox.register(EVENT_ID, ANY),
            call.__aexit__(None, None, None),
        ]
        gateway.charge.assert_not_called()  # повтор не читает платёж и не идёт в шлюз
        assert result is False

    async def test__conflicting_event(
        self, service: services.Payment, uow: MagicMock, gateway: Mock
    ) -> None:
        # Setup
        uow.inbox.register.side_effect = errors.MessageConflict(message_id=EVENT_ID)

        # Call and Assert
        with pytest.raises(errors.MessageConflict):
            await service.process(EVENT)

        uow.payments.fetch_by_id.assert_not_called()
        gateway.charge.assert_not_called()

    @pytest.mark.parametrize(
        "status", [entities.PaymentStatus.SUCCEEDED, entities.PaymentStatus.FAILED]
    )
    async def test__already_processed(
        self,
        service: services.Payment,
        uow: MagicMock,
        gateway: Mock,
        status: entities.PaymentStatus,
    ) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = _stored_payment(status)

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed) as exc_info:
            await service.process(EVENT)

        assert exc_info.value.context["action"] == "process"
        gateway.charge.assert_not_called()  # обработанный платёж через шлюз не проводится
        uow.outbox.add.assert_not_called()
        uow.commit.assert_not_called()

    async def test__not_found(
        self, service: services.Payment, uow: MagicMock, gateway: Mock
    ) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = None

        # Call and Assert
        with pytest.raises(errors.PaymentNotFound):
            await service.process(EVENT)

        gateway.charge.assert_not_called()
        uow.commit.assert_not_called()

    async def test__storage_failed_in_gateway_call(
        self, service: services.Payment, uow: MagicMock, gateway: Mock
    ) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = _stored_payment()
        gateway.charge.side_effect = errors.StorageUnavailable()

        # Call and Assert
        with pytest.raises(errors.StorageUnavailable):
            await service.process(EVENT)

        uow.outbox.add.assert_not_called()
        uow.commit.assert_not_called()


class TestNotify:
    async def test__notified(
        self, service: services.Payment, uow: MagicMock, notifier: Mock
    ) -> None:
        # Setup
        payment = _stored_payment(entities.PaymentStatus.SUCCEEDED)
        uow.payments.fetch_by_id.return_value = payment
        manager = Mock()
        manager.attach_mock(uow, "uow")
        manager.attach_mock(notifier, "notifier")

        # Call
        result = await service.notify(EVENT)

        # Assert
        assert manager.mock_calls == [
            call.uow.__aenter__(),
            call.uow.inbox.register(EVENT_ID, ANY),
            call.uow.payments.fetch_by_id(PAYMENT_ID, for_update=True),
            call.notifier.notify(ANY),  # отметка о доставке — только после ответа получателя
            call.uow.commit(),
            call.uow.__aexit__(None, None, None),
        ]
        notified: dtos.PaymentInfo = notifier.notify.call_args.args[0]
        assert (notified.id, notified.status) == (PAYMENT_ID, entities.PaymentStatus.SUCCEEDED)
        assert payment.notified_at == NOW
        assert result is True

    async def test__duplicate_event(
        self, service: services.Payment, uow: MagicMock, notifier: Mock
    ) -> None:
        # Setup
        uow.inbox.register.return_value = False

        # Call
        result = await service.notify(EVENT)

        # Assert
        assert uow.mock_calls == [
            call.__aenter__(),
            call.inbox.register(EVENT_ID, ANY),
            call.__aexit__(None, None, None),
        ]
        notifier.notify.assert_not_called()
        assert result is False

    async def test__conflicting_event(
        self, service: services.Payment, uow: MagicMock, notifier: Mock
    ) -> None:
        # Setup
        uow.inbox.register.side_effect = errors.MessageConflict(message_id=EVENT_ID)

        # Call and Assert
        with pytest.raises(errors.MessageConflict):
            await service.notify(EVENT)

        uow.payments.fetch_by_id.assert_not_called()
        notifier.notify.assert_not_called()
        uow.commit.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "notified_at"),
        [
            (entities.PaymentStatus.PENDING, None),
            (entities.PaymentStatus.FAILED, NOW),
        ],
        ids=["pending", "already_notified"],
    )
    async def test__not_notifiable(
        self,
        service: services.Payment,
        uow: MagicMock,
        notifier: Mock,
        status: entities.PaymentStatus,
        notified_at: datetime | None,
    ) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = _stored_payment(status, notified_at=notified_at)

        # Call and Assert
        with pytest.raises(errors.PaymentTransitionNotAllowed) as exc_info:
            await service.notify(EVENT)

        assert exc_info.value.context["action"] == "notify"
        notifier.notify.assert_not_called()
        uow.commit.assert_not_called()

    async def test__not_found(
        self, service: services.Payment, uow: MagicMock, notifier: Mock
    ) -> None:
        # Setup
        uow.payments.fetch_by_id.return_value = None

        # Call and Assert
        with pytest.raises(errors.PaymentNotFound):
            await service.notify(EVENT)

        notifier.notify.assert_not_called()

    @pytest.mark.parametrize(
        "error",
        [
            errors.WebhookUnavailable(url="http://webhook_sink:8080/hook"),
            errors.WebhookRejected(url="http://webhook_sink:8080/hook", status_code=404),
            errors.WebhookAddressInvalid(url="http://webhook_sink:8080/hook"),
        ],
    )
    async def test__delivery_failed(
        self, service: services.Payment, uow: MagicMock, notifier: Mock, error: errors.Error
    ) -> None:
        # Setup
        payment = _stored_payment(entities.PaymentStatus.SUCCEEDED)
        uow.payments.fetch_by_id.return_value = payment
        notifier.notify.side_effect = error

        # Call and Assert
        with pytest.raises(type(error)):
            await service.notify(EVENT)

        assert payment.notified_at is None  # ни отметки, ни регистрации: повтор доставит снова
        uow.commit.assert_not_called()


class TestFingerprint:
    async def test__other_event_id_same_content(
        self, service: services.Payment, uow: MagicMock
    ) -> None:
        # Setup
        uow.inbox.register.return_value = False
        await service.process(EVENT)
        first = _fingerprint(uow)
        uow.inbox.register.reset_mock()

        # Call
        await service.process(EVENT.model_copy(update={"event_id": UUID(int=99)}))

        # Assert
        assert _fingerprint(uow) == first  # идентификатор события — ключ, а не содержимое

    async def test__other_payment(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.inbox.register.return_value = False
        await service.process(EVENT)
        first = _fingerprint(uow)
        uow.inbox.register.reset_mock()

        # Call
        await service.process(EVENT.model_copy(update={"payment_id": UUID(int=2)}))

        # Assert
        assert _fingerprint(uow) != first

    async def test__scenarios_differ(self, service: services.Payment, uow: MagicMock) -> None:
        # Setup
        uow.inbox.register.return_value = False
        await service.process(EVENT)
        processed = _fingerprint(uow)
        uow.inbox.register.reset_mock()

        # Call
        await service.notify(EVENT)

        # Assert
        assert _fingerprint(uow) != processed  # событие не того типа — конфликт, а не дубль
