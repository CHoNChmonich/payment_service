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
    async def process(self, payment_id: UUID) -> dtos.PaymentInfo:
        """Проводит платёж через шлюз и фиксирует результат — идемпотентно по статусу.

        Платёж в обработке получает ``succeeded`` или, если шлюз отказал (после повторов
        реализации порта ``PaymentGateway``), ``failed``. Уже обработанный платёж через шлюз не
        прогоняется: возвращается как есть.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Платёж в финальном статусе.

        Raises:
            PaymentNotFound: Платежа с таким идентификатором нет.
            StorageUnavailable: Временный сбой хранилища.
        """

    @abstractmethod
    async def notify(self, payment_id: UUID) -> bool:
        """Уведомляет клиента о результате платежа по его ``webhook_url`` — не более одного раза.

        Args:
            payment_id: Идентификатор обработанного платежа.

        Returns:
            ``True`` — уведомление доставлено этим вызовом; ``False`` — уже было доставлено.

        Raises:
            PaymentNotFound: Платежа с таким идентификатором нет.
            PaymentNotProcessed: Платёж ещё в обработке.
            WebhookUnavailable: Получатель недоступен и после повторов реализации порта
                ``WebhookNotifier``; отметка о доставке не записана.
            WebhookRejected: Получатель отверг уведомление; отметка о доставке не записана.
            StorageUnavailable: Временный сбой хранилища.
        """
