"""События платежа — факты сценариев для очередей обработки (transactional outbox).

Сервис пишет событие в outbox в той же транзакции, что и изменение платежа, поэтому
опубликовано будет ровно то, что зафиксировано. ``event_id`` рождается вместе с фактом и служит
потребителям ключом идемпотентности (inbox): доставка — не менее одного раза. Имена — факт в
прошедшем времени (``PaymentCreated``); ``type`` — дискриминатор, по которому событие
восстанавливается из хранилища и маршрутизируется публикатором.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal, get_args
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

type PaymentEventType = Literal["payment_created", "payment_processed"]
"""Дискриминатор события платежа."""

PAYMENT_EVENT_TYPES: tuple[PaymentEventType, ...] = get_args(PaymentEventType.__value__)
"""Все типы событий: relay доставляет каждый своей дорожкой."""


class PaymentEvent(BaseModel):
    """Общие поля событий платежа.

    Attributes:
        event_id: Идентификатор события — ключ идемпотентности у потребителей.
        occurred_at: Момент сценария, породившего событие.
        payment_id: Платёж, с которым произошло событие.
    """

    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payment_id: UUID


class PaymentCreated(PaymentEvent):
    """Платёж создан и ждёт проведения через шлюз.

    Attributes:
        type: Дискриминатор события.
    """

    type: Literal["payment_created"] = "payment_created"


class PaymentProcessed(PaymentEvent):
    """Платёж проведён: статус финальный, клиента пора уведомить.

    Attributes:
        type: Дискриминатор события.
    """

    type: Literal["payment_processed"] = "payment_processed"


AnyPaymentEvent = Annotated[PaymentCreated | PaymentProcessed, Field(discriminator="type")]


class RelayResult(BaseModel):
    """Итог одного вызова доставки outbox.

    Attributes:
        published: Сколько событий опубликовано и удалено.
        unavailable: Типы событий, чью пачку доставить не удалось (временный сбой); их события
            остались в outbox.
    """

    model_config = ConfigDict(frozen=True)

    published: int = Field(default=0, ge=0)
    unavailable: tuple[PaymentEventType, ...] = ()
