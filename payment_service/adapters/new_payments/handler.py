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
    """Тело сообщения → идентификатор платежа → сценарии; о RabbitMQ и повторах не знает.

    Попытка обработки — ``process`` (шлюз, статус) и затем ``notify`` (webhook). Шаги
    идемпотентны по состоянию платежа: при повторной доставке уже обработанный платёж через шлюз
    не прогоняется, доставленный webhook не отправляется заново.

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
        """Разбирает событие, проводит платёж и уведомляет клиента.

        Args:
            body: Тело сообщения.

        Returns:
            ``True`` — webhook доставлен сейчас; ``False`` — был доставлен ранее (повторная
            доставка сообщения).

        Raises:
            InvalidMessage: Тело не по контракту; ошибки сценариев — по контракту порта.
        """
        try:
            payment_id = PaymentCreatedMessage.model_validate_json(body).to_application()
        except ValidationError as exc:
            raise InvalidMessage(f"Сообщение не по контракту: {exc}") from exc
        await self.payment.process(payment_id)
        return await self.payment.notify(payment_id)
