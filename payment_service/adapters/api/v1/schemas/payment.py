"""Тела запросов API v1 для платежей."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from payment_service.application import dtos as app_dtos
from payment_service.application import entities


class Currency(StrEnum):
    """Валюта на HTTP-краю; ``entities.Currency`` наружу не выдаётся."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"

    def to_application(self) -> entities.Currency:
        """Переводит валюту шва в валюту ядра.

        Returns:
            Член ``entities.Currency`` с тем же значением.
        """
        return entities.Currency(self.value)

    @classmethod
    def from_application(cls, currency: entities.Currency) -> "Currency":
        """Переводит валюту ядра в валюту шва.

        Args:
            currency: Валюта из ядра.

        Returns:
            Член перечисления шва с тем же значением.
        """
        return cls(currency.value)


class PaymentCreateBody(BaseModel):
    """Тело запроса на создание платежа; ключ идемпотентности приходит заголовком.

    Ограничения полей — те же типы, что у DTO ядра (``dtos.base``): тело, прошедшее валидацию
    на границе, гарантированно принимается ``to_application()``.

    Attributes:
        amount: Сумма больше нуля, не более двух знаков после запятой; число или строка.
        currency: Валюта платежа.
        description: Назначение платежа; может быть пустым.
        metadata: Произвольный JSON-объект клиента; хранится и возвращается как есть.
        webhook_url: Адрес ``http``/``https`` для уведомления о результате; без учётных данных.
    """

    model_config = ConfigDict(extra="forbid")

    amount: app_dtos.Amount = Field(examples=["100.50"])
    currency: Currency = Field(examples=["RUB"])
    description: app_dtos.Description = Field(default="", examples=["Заказ №42"])
    metadata: app_dtos.Metadata = Field(default_factory=dict, examples=[{"order_id": 42}])
    webhook_url: app_dtos.WebhookUrl = Field(examples=["https://shop.example.com/hooks/payments"])

    def to_application(self, idempotency_key: str) -> app_dtos.NewPaymentInfo:
        """Переводит тело запроса и ключ идемпотентности в DTO ядра.

        Args:
            idempotency_key: Значение заголовка ``Idempotency-Key``, прошедшее валидацию.

        Returns:
            Данные для создания платежа.
        """
        return app_dtos.NewPaymentInfo(
            idempotency_key=idempotency_key,
            amount=self.amount,
            currency=self.currency.to_application(),
            description=self.description,
            metadata=self.metadata,
            webhook_url=self.webhook_url,
        )
