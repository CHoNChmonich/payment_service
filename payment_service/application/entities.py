"""Domain слой: перечисления, состояния платежа (State), сущность платежа.

Сущность — dataclass с тем поведением, которое принадлежит ей самой: переходы статуса, отметка
об уведомлении, сравнение с повторным запросом. Персистентность о сущности знает только через
императивный маппинг в ``adapters/database/mapping.py``; сюда SQLAlchemy не проникает.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from payment_service.application import errors


class Currency(StrEnum):
    """Валюта платежа; в БД хранится значение члена."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Статус платежа; в БД хранится значение члена (``pending``, ``succeeded``, ``failed``)."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


# --- Состояния платежа ----------------------------------------------------------------


class _PaymentState(ABC):
    """Паттерн: State (GoF). Поведение платежа, зависящее от его статуса.

    Методы ``Payment`` делегируют объекту состояния; недопустимое действие —
    ``PaymentTransitionNotAllowed``. Проверки ``ensure_*`` идут до внешнего эффекта (шлюз,
    webhook): недопустимое действие отклоняется, не начавшись. Новый статус = новый класс
    состояния + строка в ``_STATES``.
    """

    @abstractmethod
    def ensure_processable(self, payment: Payment) -> None:
        """Проверяет, что платёж можно проводить через шлюз.

        Args:
            payment: Платёж, который собираются провести.

        Raises:
            PaymentTransitionNotAllowed: Платёж уже обработан.
        """

    @abstractmethod
    def succeed(self, payment: Payment, now: datetime) -> None:
        """Переводит платёж в статус «успешен».

        Args:
            payment: Платёж, статус которого меняется.
            now: Момент обработки.

        Raises:
            PaymentTransitionNotAllowed: Из этого состояния переход невозможен.
        """

    @abstractmethod
    def fail(self, payment: Payment, now: datetime) -> None:
        """Переводит платёж в статус «неуспешен».

        Args:
            payment: Платёж, статус которого меняется.
            now: Момент обработки.

        Raises:
            PaymentTransitionNotAllowed: Из этого состояния переход невозможен.
        """

    @abstractmethod
    def ensure_notifiable(self, payment: Payment) -> None:
        """Проверяет, что клиента можно уведомить о результате.

        Args:
            payment: Платёж, о котором собираются уведомить.

        Raises:
            PaymentTransitionNotAllowed: Результата ещё нет или клиент уже уведомлён.
        """

    def mark_notified(self, payment: Payment, now: datetime) -> None:
        """Отмечает, что клиент уведомлён о результате; правило — ``ensure_notifiable``.

        Args:
            payment: Платёж, о котором доставлено уведомление.
            now: Момент доставки.

        Raises:
            PaymentTransitionNotAllowed: Результата ещё нет или клиент уже уведомлён.
        """
        self.ensure_notifiable(payment)
        payment.notified_at = now


class _PendingState(_PaymentState):
    """Платёж в обработке: может стать успешным или неуспешным; уведомлять ещё не о чем."""

    def ensure_processable(self, payment: Payment) -> None:
        """Платёж в обработке можно проводить.

        Args:
            payment: Платёж в статусе ``pending``.
        """

    def succeed(self, payment: Payment, now: datetime) -> None:
        """Фиксирует успех платежа.

        Args:
            payment: Платёж в статусе ``pending``.
            now: Момент обработки.
        """
        payment.status = PaymentStatus.SUCCEEDED
        payment.processed_at = now

    def fail(self, payment: Payment, now: datetime) -> None:
        """Фиксирует отказ платежа.

        Args:
            payment: Платёж в статусе ``pending``.
            now: Момент обработки.
        """
        payment.status = PaymentStatus.FAILED
        payment.processed_at = now

    def ensure_notifiable(self, payment: Payment) -> None:
        """Отклоняет уведомление: результата ещё нет.

        Args:
            payment: Платёж в статусе ``pending``.

        Raises:
            PaymentTransitionNotAllowed: Всегда.
        """
        raise errors.PaymentTransitionNotAllowed(
            id=payment.id, status=payment.status, action="notify"
        )


class _FinalState(_PaymentState):
    """Обработанный платёж: статус больше не меняется, уведомление — один раз."""

    def ensure_processable(self, payment: Payment) -> None:
        """Отклоняет повторное проведение через шлюз.

        Args:
            payment: Платёж в статусе ``succeeded`` или ``failed``.

        Raises:
            PaymentTransitionNotAllowed: Всегда.
        """
        raise errors.PaymentTransitionNotAllowed(
            id=payment.id, status=payment.status, action="process"
        )

    def succeed(self, payment: Payment, now: datetime) -> None:
        """Отклоняет повторную обработку.

        Args:
            payment: Платёж в статусе ``succeeded`` или ``failed``.
            now: Момент; не используется.

        Raises:
            PaymentTransitionNotAllowed: Всегда.
        """
        raise errors.PaymentTransitionNotAllowed(
            id=payment.id, status=payment.status, action="succeed"
        )

    def fail(self, payment: Payment, now: datetime) -> None:
        """Отклоняет повторную обработку.

        Args:
            payment: Платёж в статусе ``succeeded`` или ``failed``.
            now: Момент; не используется.

        Raises:
            PaymentTransitionNotAllowed: Всегда.
        """
        raise errors.PaymentTransitionNotAllowed(
            id=payment.id, status=payment.status, action="fail"
        )

    def ensure_notifiable(self, payment: Payment) -> None:
        """Допускает уведомление, если клиент ещё не уведомлён.

        Args:
            payment: Платёж в финальном статусе.

        Raises:
            PaymentTransitionNotAllowed: Клиент уже уведомлён.
        """
        if payment.notified_at is not None:
            raise errors.PaymentTransitionNotAllowed(
                id=payment.id, status=payment.status, action="notify"
            )


_STATES: dict[PaymentStatus, _PaymentState] = {
    PaymentStatus.PENDING: _PendingState(),
    PaymentStatus.SUCCEEDED: _FinalState(),
    PaymentStatus.FAILED: _FinalState(),
}


# --- Сущности -------------------------------------------------------------------------


@dataclass(kw_only=True)
class Payment:
    """Платёж.

    Жизненный цикл: ``pending`` → ``succeeded`` | ``failed`` (решает результат обращения к
    шлюзу), затем клиент уведомляется о результате один раз (``notified_at``). Сравнение с
    повторным запросом (``matches``) защищает от подмены данных под тем же ключом
    идемпотентности.

    Attributes:
        id: Идентификатор; присваивается при создании, до сохранения.
        idempotency_key: Ключ идемпотентности запроса на создание; уникален среди платежей.
        amount: Сумма платежа.
        currency: Валюта.
        description: Назначение платежа.
        metadata: Произвольные данные клиента.
        webhook_url: Адрес уведомления о результате.
        status: Текущий статус.
        created_at: Момент создания.
        processed_at: Момент перехода в финальный статус; ``None`` в обработке.
        notified_at: Момент доставки уведомления; ``None``, пока не доставлено.
    """

    id: UUID = field(default_factory=uuid4)
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    webhook_url: str
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    processed_at: datetime | None = None
    notified_at: datetime | None = None

    def ensure_processable(self) -> None:
        """Проверяет до обращения к шлюзу, что платёж ещё в обработке.

        Raises:
            PaymentTransitionNotAllowed: Платёж уже обработан.
        """
        _STATES[self.status].ensure_processable(self)

    def succeed(self, now: datetime) -> None:
        """Фиксирует успех платежа (``pending → succeeded``).

        Args:
            now: Момент обработки.

        Raises:
            PaymentTransitionNotAllowed: Платёж уже обработан.
        """
        _STATES[self.status].succeed(self, now)

    def fail(self, now: datetime) -> None:
        """Фиксирует отказ платежа (``pending → failed``).

        Args:
            now: Момент обработки.

        Raises:
            PaymentTransitionNotAllowed: Платёж уже обработан.
        """
        _STATES[self.status].fail(self, now)

    def ensure_notifiable(self) -> None:
        """Проверяет до отправки уведомления, что результат есть и клиент ещё не уведомлён.

        Raises:
            PaymentTransitionNotAllowed: Платёж не обработан или клиент уже уведомлён.
        """
        _STATES[self.status].ensure_notifiable(self)

    def mark_notified(self, now: datetime) -> None:
        """Запоминает доставку уведомления о результате — не более одного раза.

        Args:
            now: Момент доставки.

        Raises:
            PaymentTransitionNotAllowed: Платёж не обработан или клиент уже уведомлён.
        """
        _STATES[self.status].mark_notified(self, now)

    def matches(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
    ) -> bool:
        """Совпадают ли значимые поля с повторным запросом под тем же ключом идемпотентности.

        Сумма сравнивается как число (``10.0`` и ``10.00`` равны), метаданные — как словари
        (порядок ключей не важен), адрес — как строка после нормализации на границе.

        Args:
            amount: Сумма повторного запроса.
            currency: Валюта повторного запроса.
            description: Назначение повторного запроса.
            metadata: Метаданные повторного запроса.
            webhook_url: Адрес уведомления повторного запроса.

        Returns:
            ``True`` — тот же запрос; ``False`` — под тем же ключом прислали другие данные.
        """
        return (
            self.amount == amount
            and self.currency is currency
            and self.description == description
            and self.metadata == metadata
            and self.webhook_url == webhook_url
        )
