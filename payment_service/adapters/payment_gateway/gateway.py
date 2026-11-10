"""Эмуляция платёжного шлюза: задержка 2–5 с и отказ с заданной вероятностью."""

import asyncio
import logging
from random import Random, SystemRandom
from uuid import UUID

from payment_service.adapters import retry
from payment_service.application import dtos, errors, interfaces

from .settings import Settings

logger = logging.getLogger(__name__)


class PaymentGateway(interfaces.PaymentGateway):
    """Реализация порта шлюза без внешнего сервиса: ждёт случайное время и иногда отказывает.

    Отказ — ``GatewayUnavailable``: шлюз «не ответил» или «отклонил»; различать их задание не
    требует. Повторы при отказе — дело декоратора ``RetryingPaymentGateway``. Контракт порта
    (идемпотентность по ``payment.id``) эмуляция выполняет в памяти процесса: принятый платёж
    при повторном вызове принимается сразу, без задержки и без нового броска на отказ.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        random: Random | None = None,
        sleep: retry.Sleep = asyncio.sleep,
    ) -> None:
        """Запоминает параметры эмуляции.

        Args:
            settings: Границы задержки и доля отказов.
            random: Источник случайности; по умолчанию системный, в тестах — с зерном.
            sleep: Пауза; в тестах подменяется.
        """
        self._settings = settings
        self._random = random or SystemRandom()
        self._sleep = sleep
        self._accepted: set[UUID] = set()

    async def charge(self, payment: dtos.PaymentInfo) -> None:
        """Эмулирует обращение к шлюзу.

        Args:
            payment: Платёж в обработке.

        Raises:
            GatewayUnavailable: Шлюз отказал (с вероятностью ``PAYMENT_GATEWAY_FAILURE_RATE``).
        """
        if payment.id in self._accepted:
            logger.info("Платёж %s уже принят шлюзом — повторный вызов", payment.id)
            return
        delay = self._random.uniform(
            self._settings.PAYMENT_GATEWAY_DELAY_MIN, self._settings.PAYMENT_GATEWAY_DELAY_MAX
        )
        await self._sleep(delay)
        if self._random.random() < self._settings.PAYMENT_GATEWAY_FAILURE_RATE:
            logger.info("Шлюз отказал по платежу %s через %.1f с", payment.id, delay)
            raise errors.GatewayUnavailable()
        self._accepted.add(payment.id)
        logger.info("Шлюз принял платёж %s через %.1f с", payment.id, delay)
