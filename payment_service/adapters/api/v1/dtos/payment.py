"""Ответы API v1 для платежей."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from payment_service.adapters.api.v1.schemas.payment import Currency
from payment_service.application import dtos as app_dtos
from payment_service.application import entities

# Контракт API: деньги — строка с двумя знаками, какой бы ни была точность Decimal внутри.
Money = Annotated[Decimal, PlainSerializer(lambda value: f"{value:.2f}", return_type=str)]


class PaymentStatus(StrEnum):
    """Статус платежа на HTTP-краю; ``entities.PaymentStatus`` наружу не выдаётся."""

    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"

    @classmethod
    def from_application(cls, status: entities.PaymentStatus) -> "PaymentStatus":
        """Переводит статус ядра в статус шва.

        Args:
            status: Статус платежа из ядра.

        Returns:
            Член перечисления шва с тем же значением.
        """
        return cls(status.value)


class PaymentAcceptedResponse(BaseModel):
    """Ответ на принятый в обработку платёж (202).

    Attributes:
        payment_id: Идентификатор платежа для опроса статуса.
        status: Статус на момент ответа; у повторного запроса может быть уже финальным.
        created_at: Момент создания платежа в UTC.
    """

    model_config = ConfigDict(extra="forbid")

    payment_id: UUID = Field(examples=["7c9e6679-7425-40de-944b-e07fc1f90ae7"])
    status: PaymentStatus = Field(examples=["pending"])
    created_at: datetime = Field(examples=["2026-11-10T12:00:00Z"])

    @classmethod
    def from_application(cls, result: app_dtos.PaymentInfo) -> "PaymentAcceptedResponse":
        """Строит ответ из DTO ядра.

        Args:
            result: Платёж, возвращённый сценарием создания.

        Returns:
            Модель ответа.
        """
        return cls(
            payment_id=result.id,
            status=PaymentStatus.from_application(result.status),
            created_at=result.created_at,
        )


class PaymentResponse(BaseModel):
    """Платёж в ответе API.

    Attributes:
        payment_id: Идентификатор платежа.
        idempotency_key: Ключ идемпотентности, с которым платёж был создан.
        amount: Сумма строкой с двумя знаками.
        currency: Валюта платежа.
        description: Назначение платежа.
        metadata: Метаданные клиента как были переданы.
        webhook_url: Адрес уведомления о результате.
        status: Статус платежа.
        created_at: Момент создания в UTC.
        processed_at: Момент получения результата от шлюза; ``null``, пока платёж в обработке.
        notified_at: Момент доставки webhook; ``null``, пока уведомление не доставлено.
    """

    model_config = ConfigDict(extra="forbid")

    payment_id: UUID = Field(examples=["7c9e6679-7425-40de-944b-e07fc1f90ae7"])
    idempotency_key: str = Field(examples=["order-42-attempt-1"])
    amount: Money = Field(examples=["100.50"])
    currency: Currency = Field(examples=["RUB"])
    description: str = Field(examples=["Заказ №42"])
    metadata: dict[str, Any] = Field(examples=[{"order_id": 42}])
    webhook_url: str = Field(examples=["https://shop.example.com/hooks/payments"])
    status: PaymentStatus = Field(examples=["succeeded"])
    created_at: datetime = Field(examples=["2026-11-10T12:00:00Z"])
    processed_at: datetime | None = Field(examples=["2026-11-10T12:00:04Z"])
    notified_at: datetime | None = Field(examples=["2026-11-10T12:00:05Z"])

    @classmethod
    def from_application(cls, result: app_dtos.PaymentInfo) -> "PaymentResponse":
        """Строит ответ из DTO ядра.

        Args:
            result: Платёж, возвращённый сервисом.

        Returns:
            Модель ответа.
        """
        return cls(
            payment_id=result.id,
            idempotency_key=result.idempotency_key,
            amount=result.amount,
            currency=Currency.from_application(result.currency),
            description=result.description,
            metadata=result.metadata,
            webhook_url=result.webhook_url,
            status=PaymentStatus.from_application(result.status),
            created_at=result.created_at,
            processed_at=result.processed_at,
            notified_at=result.notified_at,
        )
