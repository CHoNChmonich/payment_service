"""Декоратор повторов над портом уведомлений."""

import asyncio
from functools import partial

from payment_service.adapters import retry
from payment_service.application import dtos, errors, interfaces


class RetryingWebhookNotifier(interfaces.WebhookNotifier):
    """Паттерн Decorator (GoF): повторяет доставку webhook при временной недоступности.

    Повторяется только ``UnavailableError``; ``WebhookRejected`` (получатель отверг) уходит
    наружу сразу. После последней неудачной попытки наружу уходит её исключение, и консьюмер
    кладёт сообщение в DLQ.
    """

    def __init__(
        self,
        inner: interfaces.WebhookNotifier,
        policy: retry.Retry,
        *,
        sleep: retry.Sleep = asyncio.sleep,
    ) -> None:
        """Оборачивает реализацию порта политикой повторов.

        Args:
            inner: Настоящий отправитель.
            policy: Число попыток и паузы.
            sleep: Пауза; в тестах подменяется.
        """
        self._inner = inner
        self._policy = policy
        self._sleep = sleep

    async def notify(self, payment: dtos.PaymentInfo) -> None:
        """Доставляет уведомление, повторяя при недоступности получателя.

        Args:
            payment: Обработанный платёж.

        Raises:
            WebhookUnavailable: Получатель недоступен во всех попытках.
            WebhookRejected: Получатель отверг уведомление.
        """
        await self._policy.call(
            partial(self._inner.notify, payment),
            retry_on=errors.UnavailableError,
            sleep=self._sleep,
            what=f"webhook, платёж {payment.id}",
        )
