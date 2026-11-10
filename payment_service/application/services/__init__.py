"""Сценарии использования: сервис на сущность с набором методов.

Каждый публичный метод — граница транзакции: он открывает ``async with self.uow_factory()``
и при записи вызывает ``uow.commit()``. Сервисы реализуют входящие порты
(``interfaces.Payment``, ``interfaces.EventRelay``) и зависят только от исходящих портов
``interfaces``. ``EventRelay`` — исключение из «сервис на сущность»: его сценарий — доставка
событий outbox, а не сущность.
"""

from .event_relay import EventRelay
from .payment import Payment

__all__ = ["EventRelay", "Payment"]
