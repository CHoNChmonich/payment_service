"""Исходящий порт уведомления клиента о результате платежа."""

from abc import ABC, abstractmethod

from payment_service.application import dtos


class WebhookNotifier(ABC):
    """Доставка уведомления о результате платежа на ``webhook_url`` клиента."""

    @abstractmethod
    async def notify(self, payment: dtos.PaymentInfo) -> None:
        """Доставляет уведомление; возврат без исключения — получатель подтвердил приём.

        Исключение означает окончательный отказ доставки (повторы, если есть, — дело
        реализации).

        Args:
            payment: Обработанный платёж; формат тела уведомления — дело адаптера.

        Raises:
            WebhookUnavailable: Получатель недоступен и после повторов.
            WebhookRejected: Получатель отверг уведомление.
        """
