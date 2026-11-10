"""Входящий порт сценариев платежа."""

from abc import ABC, abstractmethod
from uuid import UUID

from payment_service.application import dtos


class Payment(ABC):
    """Сценарии платежа — входящий порт.

    Контроллер и обработчик сообщений зависят от этого порта, а не от ``services.Payment``:
    так сценарий можно обернуть или подменить без правки сервиса. Контракт методов
    (аргументы, результат, ошибки) описан здесь; реализация описывает только особенности
    исполнения.
    """

    @abstractmethod
    async def create(self, new_payment_info: dtos.NewPaymentInfo) -> dtos.PaymentInfo:
        """Принимает платёж в обработку — идемпотентно по ``idempotency_key``.

        Новый ключ — платёж сохраняется в статусе ``pending`` и событие ``PaymentCreated``
        попадает в outbox той же транзакцией. Повтор запроса с тем же ключом и теми же данными
        возвращает исходный платёж, ничего не меняя.

        Args:
            new_payment_info: Ключ идемпотентности и данные платежа.

        Returns:
            Созданный или ранее созданный платёж.

        Raises:
            IdempotencyKeyConflict: Под этим ключом уже создан платёж с другими данными.
            StorageUnavailable: Временный сбой хранилища.
        """

    @abstractmethod
    async def get(self, payment_id: UUID) -> dtos.PaymentInfo:
        """Возвращает платёж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Найденный платёж.

        Raises:
            PaymentNotFound: Платежа с таким идентификатором нет.
            StorageUnavailable: Временный сбой хранилища.
        """

    @abstractmethod
    async def process(self, event: dtos.PaymentEventInfo) -> bool:
        """Проводит платёж через шлюз по событию «платёж создан» — один раз на событие.

        Событие регистрируется в inbox той же транзакцией, что и результат: повтор события
        (повторная доставка) платёж не трогает. Платёж в обработке получает ``succeeded`` или,
        если шлюз отказал (после повторов реализации порта ``PaymentGateway``), ``failed``;
        событие ``PaymentProcessed`` попадает в outbox той же транзакцией.

        Args:
            event: Событие о созданном платеже.

        Returns:
            ``True`` — платёж проведён этим вызовом; ``False`` — событие уже обработано.

        Raises:
            PaymentNotFound: Платежа с таким идентификатором нет.
            PaymentTransitionNotAllowed: Платёж уже обработан (другим событием).
            MessageConflict: Событие с этим ``event_id`` уже обработано для другого платежа.
            StorageUnavailable: Временный сбой хранилища.
        """

    @abstractmethod
    async def notify(self, event: dtos.PaymentEventInfo) -> bool:
        """Уведомляет клиента о результате по событию «платёж проведён».

        Событие регистрируется в inbox той же транзакцией, что и отметка о доставке: повтор
        зафиксированного события уведомление не повторяет, отметка о доставке — не более одной.
        Сам webhook уходит не менее одного раза: сбой фиксации после ответа 2xx откатывает
        отметку и регистрацию, повтор отправит его снова. Уведомление уходит на
        ``webhook_url`` платежа.

        Args:
            event: Событие о проведённом платеже.

        Returns:
            ``True`` — уведомление доставлено этим вызовом; ``False`` — событие уже обработано.

        Raises:
            PaymentNotFound: Платежа с таким идентификатором нет.
            PaymentTransitionNotAllowed: Платёж ещё в обработке или клиент уже уведомлён.
            WebhookUnavailable: Получатель недоступен и после повторов реализации порта
                ``WebhookNotifier``; отметка о доставке не записана.
            WebhookRejected: Получатель отверг уведомление; отметка о доставке не записана.
            WebhookAddressInvalid: Адрес непригоден для доставки; отметка не записана.
            MessageConflict: Событие с этим ``event_id`` уже обработано для другого платежа.
            StorageUnavailable: Временный сбой хранилища.
        """
