"""Декоратор повторов над портом шлюза."""

import asyncio
from functools import partial

from payment_service.adapters import retry
from payment_service.application import dtos, errors, interfaces


class RetryingPaymentGateway(interfaces.PaymentGateway):
    """Паттерн Decorator (GoF): повторяет обращение к шлюзу при временном отказе.

    Повторяется только ``UnavailableError``; после последней неудачной попытки наружу уходит
    её исключение, и сервис переводит платёж в ``failed``. Ядро и консьюмер о повторах не
    знают; политика собирается в composition root из настроек шлюза.
    """

    def __init__(
        self,
        inner: interfaces.PaymentGateway,
        policy: retry.Retry,
        *,
        sleep: retry.Sleep = asyncio.sleep,
    ) -> None:
        """Оборачивает реализацию порта политикой повторов.

        Args:
            inner: Настоящий шлюз.
            policy: Число попыток и паузы.
            sleep: Пауза; в тестах подменяется.
        """
        self._inner = inner
        self._policy = policy
        self._sleep = sleep

    async def charge(self, payment: dtos.PaymentInfo) -> None:
        """Проводит платёж, повторяя обращение при отказе шлюза.

        Args:
            payment: Платёж в обработке.

        Raises:
            GatewayUnavailable: Шлюз отказал во всех попытках.
        """
        await self._policy.call(
            partial(self._inner.charge, payment),
            retry_on=errors.UnavailableError,
            sleep=self._sleep,
            what=f"шлюз, платёж {payment.id}",
        )
