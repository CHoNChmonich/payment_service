"""Тела запросов API v1 (``*Body``); ``to_application()`` переводит их в DTO ядра.

Модели ядра наружу не торчат: у шва свои классы и своё перечисление валюты.
"""

from .payment import Currency, PaymentCreateBody

__all__ = ["Currency", "PaymentCreateBody"]
