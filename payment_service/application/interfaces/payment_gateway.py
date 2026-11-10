"""Исходящий порт платёжного шлюза."""

from abc import ABC, abstractmethod

from payment_service.application import dtos


class PaymentGateway(ABC):
    """Проведение платежа во внешнем платёжном шлюзе."""

    @abstractmethod
    async def charge(self, payment: dtos.PaymentInfo) -> None:
        """Проводит платёж; возврат без исключения — платёж принят шлюзом.

        Исключение означает окончательный отказ реализации (повторы, если есть, — её дело):
        сервис переводит платёж в ``failed``.

        Args:
            payment: Платёж в обработке.

        Raises:
            GatewayUnavailable: Шлюз отказал или недоступен и после повторов.
        """
