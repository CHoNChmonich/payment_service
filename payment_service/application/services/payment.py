"""Сценарии платежа."""

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from payment_service.application import dtos, entities, errors, interfaces

type Clock = Callable[[], datetime]

# Версия формата отпечатка inbox: смена набора полей — новая версия, а не ложный конфликт.
_FINGERPRINT_VERSION = "1"


def _utc_now() -> datetime:
    """Текущее время в UTC — часы по умолчанию.

    Returns:
        Момент вызова с зоной UTC.
    """
    return datetime.now(UTC)


class Payment(interfaces.Payment):
    """Сценарии платежа; каждый публичный метод — отдельная транзакция.

    Создание пишет платёж и событие ``PaymentCreated`` в outbox одной транзакцией. Обработка и
    уведомление вызываются событиями и первой операцией регистрируют событие в inbox: копия
    того же события в параллельной транзакции ждёт исхода первой и получает «уже обработано».
    Затем строка платежа блокируется ``FOR UPDATE`` до конца транзакции — на время обращения к
    шлюзу или к получателю webhook. Контракт методов — у порта ``interfaces.Payment``; здесь —
    как он исполняется.

    Attributes:
        uow_factory: Вызываемый объект, создающий новый Unit of Work на каждый вызов метода.
        gateway: Платёжный шлюз (с повторами, если их добавил composition root).
        notifier: Доставка webhook (с повторами, если их добавил composition root).
    """

    def __init__(
        self,
        uow_factory: interfaces.UnitOfWorkFactory,
        gateway: interfaces.PaymentGateway,
        notifier: interfaces.WebhookNotifier,
        clock: Clock = _utc_now,
    ) -> None:
        """Принимает зависимости из composition root.

        Args:
            uow_factory: Фабрика Unit of Work — вызываемый объект без аргументов.
            gateway: Платёжный шлюз.
            notifier: Доставка уведомлений.
            clock: Источник текущего времени; в тестах — фиксированный.
        """
        self.uow_factory = uow_factory
        self.gateway = gateway
        self.notifier = notifier
        self._clock = clock

    async def create(self, new_payment_info: dtos.NewPaymentInfo) -> dtos.PaymentInfo:
        """Вставляет платёж, если ключ свободен; иначе сверяет данные с уже созданным.

        Событие в outbox добавляется только при настоящей вставке: повтор запроса не порождает
        второй обработки.

        Args:
            new_payment_info: Ключ идемпотентности и данные платежа.

        Returns:
            Созданный или ранее созданный платёж.
        """
        payment = entities.Payment(
            idempotency_key=new_payment_info.idempotency_key,
            amount=new_payment_info.amount,
            currency=new_payment_info.currency,
            description=new_payment_info.description,
            metadata=dict(new_payment_info.metadata),
            webhook_url=str(new_payment_info.webhook_url),
            created_at=self._clock(),
        )
        async with self.uow_factory() as uow:
            if await uow.payments.add(payment):
                await uow.outbox.add(
                    dtos.PaymentCreated(payment_id=payment.id, occurred_at=payment.created_at)
                )
                await uow.commit()
                return dtos.PaymentInfo.model_validate(payment)
            existing = await uow.payments.fetch_by_idempotency_key(payment.idempotency_key)
            if existing is None:
                raise errors.StorageUnavailable()
            if not existing.matches(
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata,
                webhook_url=payment.webhook_url,
            ):
                raise errors.IdempotencyKeyConflict(idempotency_key=payment.idempotency_key)
            return dtos.PaymentInfo.model_validate(existing)

    async def get(self, payment_id: UUID) -> dtos.PaymentInfo:
        """Возвращает платёж по идентификатору; чтение без commit.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Найденный платёж.
        """
        async with self.uow_factory() as uow:
            payment = await uow.payments.fetch_by_id(payment_id)
            if payment is None:
                raise errors.PaymentNotFound(id=payment_id)
            return dtos.PaymentInfo.model_validate(payment)

    async def process(self, event: dtos.PaymentEventInfo) -> bool:
        """Регистрирует событие в inbox, блокирует платёж, проводит его через шлюз.

        Регистрация идёт первой: повтор события не читает платёж и не обращается к шлюзу.
        Проверка статуса (``ensure_processable``) — до шлюза: уже обработанный платёж через шлюз
        не проводится. Отказ шлюза (``GatewayUnavailable`` после повторов адаптера) — не сбой
        сценария, а результат платежа: ``failed``. Событие ``PaymentProcessed`` — в outbox до
        commit, под блокировкой строки.

        Args:
            event: Событие о созданном платеже.

        Returns:
            ``True`` — платёж проведён; ``False`` — событие уже обработано.
        """
        async with self.uow_factory() as uow:
            if not await uow.inbox.register(event.event_id, _fingerprint("payment_created", event)):
                return False
            payment = await _fetch_payment_for_update(uow, event.payment_id)
            payment.ensure_processable()
            try:
                await self.gateway.charge(dtos.PaymentInfo.model_validate(payment))
            except errors.GatewayUnavailable:
                outcome = payment.fail
            else:
                outcome = payment.succeed
            now = self._clock()
            outcome(now)
            await uow.outbox.add(dtos.PaymentProcessed(payment_id=payment.id, occurred_at=now))
            await uow.commit()
            return True

    async def notify(self, event: dtos.PaymentEventInfo) -> bool:
        """Регистрирует событие в inbox, блокирует платёж, доставляет уведомление и отмечает его.

        Проверка (``ensure_notifiable``) — до отправки: по платежу в обработке или уже
        уведомлённому webhook не уходит. Отметка — после подтверждённой доставки, момент
        доставки. Сбой доставки уходит наружу без commit: ни отметки, ни регистрации события —
        повтор доставит снова.

        Args:
            event: Событие о проведённом платеже.

        Returns:
            ``True`` — доставлено; ``False`` — событие уже обработано.
        """
        async with self.uow_factory() as uow:
            if not await uow.inbox.register(
                event.event_id, _fingerprint("payment_processed", event)
            ):
                return False
            payment = await _fetch_payment_for_update(uow, event.payment_id)
            payment.ensure_notifiable()
            await self.notifier.notify(dtos.PaymentInfo.model_validate(payment))
            payment.mark_notified(self._clock())
            await uow.commit()
            return True


async def _fetch_payment_for_update(
    uow: interfaces.UnitOfWork, payment_id: UUID
) -> entities.Payment:
    """Блокирует строку платежа до конца транзакции и возвращает платёж.

    Общий шаг обработки и уведомления (правило «общая логика — хелпер, принимающий uow»).

    Args:
        uow: Транзакция, в которой берётся блокировка.
        payment_id: Идентификатор платежа.

    Returns:
        Заблокированный платёж.

    Raises:
        PaymentNotFound: Платежа с таким идентификатором нет.
    """
    payment = await uow.payments.fetch_by_id(payment_id, for_update=True)
    if payment is None:
        raise errors.PaymentNotFound(id=payment_id)
    return payment


def _fingerprint(event_type: dtos.PaymentEventType, event: dtos.PaymentEventInfo) -> str:
    """Отпечаток значимого содержимого события для inbox.

    Тип события входит в отпечаток: одна таблица inbox у двух сценариев, и событие одного
    типа, попавшее не в тот сценарий, — конфликт, а не молчаливый дубль.

    Args:
        event_type: Тип события, которое обрабатывает сценарий.
        event: Событие.

    Returns:
        Версия формата и SHA-256 типа и ``payment_id``.
    """
    digest = hashlib.sha256(f"{event_type}\n{event.payment_id}".encode()).hexdigest()
    return f"{_FINGERPRINT_VERSION}:{digest}"
