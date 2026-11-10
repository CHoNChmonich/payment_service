"""Обработчик события «платёж создан» — аналог HTTP-контроллера для входящего сообщения."""

from pydantic import ValidationError

from payment_service.application import interfaces

from .messages import PaymentCreatedMessage


class InvalidMessage(ValueError):
    """Тело сообщения не соответствует контракту — аналог ``RequestValidationError`` HTTP.

    Только ошибка разбора входа: ``ValidationError`` из глубины сценария — дефект, а не
    плохое сообщение, и сюда не переводится.
    """


class NewPaymentsHandler:
    """Тело сообщения → событие → сценарий ``process``; о RabbitMQ и повторах не знает.

    Повторную доставку гасит inbox сценария по ``event_id``. Уведомление клиента — отдельное
    событие ``payment_processed`` и отдельный консьюмер.

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
        """Разбирает событие и проводит платёж.

        Args:
            body: Тело сообщения.

        Returns:
            ``True`` — платёж проведён сейчас; ``False`` — событие уже обработано (повторная
            доставка).

        Raises:
            InvalidMessage: Тело не по контракту; ошибки сценариев — по контракту порта.
        """
        try:
            event = PaymentCreatedMessage.model_validate_json(body).to_application()
        except ValidationError as exc:
            raise InvalidMessage(f"Сообщение не по контракту: {exc}") from exc
        return await self.payment.process(event)
