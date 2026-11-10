"""Обработчик события «платёж проведён» — аналог HTTP-контроллера для входящего сообщения."""

from pydantic import ValidationError

from payment_service.application import interfaces

from .messages import PaymentProcessedMessage


class InvalidMessage(ValueError):
    """Тело сообщения не соответствует контракту — аналог ``RequestValidationError`` HTTP.

    Только ошибка разбора входа: ``ValidationError`` из глубины сценария — дефект, а не
    плохое сообщение, и сюда не переводится.
    """


class ProcessedPaymentsHandler:
    """Тело сообщения → событие → сценарий ``notify``; о RabbitMQ и повторах не знает.

    Повторную доставку гасит inbox сценария по ``event_id``.

    Attributes:
        payment: Сценарии платежа.
    """

    def __init__(self, payment: interfaces.Payment) -> None:
        """Принимает порт сценариев платежа.

        Args:
            payment: Сценарии платежа.
        """
        self.payment = payment

    async def handle(self, body: bytes) -> bool:
        """Разбирает событие и уведомляет клиента о результате платежа.

        Args:
            body: Тело сообщения.

        Returns:
            ``True`` — уведомление доставлено сейчас; ``False`` — событие уже обработано
            (повторная доставка).

        Raises:
            InvalidMessage: Тело не по контракту; ошибки сценария — по контракту порта.
        """
        try:
            event = PaymentProcessedMessage.model_validate_json(body).to_application()
        except ValidationError as exc:
            raise InvalidMessage(f"Сообщение не по контракту: {exc}") from exc
        return await self.payment.notify(event)
