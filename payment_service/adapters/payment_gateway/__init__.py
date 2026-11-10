"""Исходящий адаптер платёжного шлюза: эмуляция и декоратор повторов.

Реализует порт ``interfaces.PaymentGateway``. ``gateway`` — эмуляция внешнего сервиса (задержка
и случайный отказ), ``retrying`` — Decorator с повторами на библиотеке ``retry``; собирает их
composition root каждого процесса, которому нужен сервис платежа.
"""

from .gateway import PaymentGateway
from .retrying import RetryingPaymentGateway
from .settings import Settings

__all__ = ["PaymentGateway", "RetryingPaymentGateway", "Settings"]
