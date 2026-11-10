"""Исходящий порт проверки API-ключа."""

from abc import ABC, abstractmethod


class Authenticator(ABC):
    """Проверка API-ключа запроса.

    Порт потребляет входящий адаптер (HTTP-контроллер), а не сервис: сценария за проверкой
    ключа нет. Отсутствие заголовка контроллер отклоняет сам (``CredentialsMissing``), сюда
    приходит непустой ключ.
    """

    @abstractmethod
    async def authenticate(self, api_key: str) -> None:
        """Проверяет ключ; возврат без исключения — запрос аутентифицирован.

        Args:
            api_key: Ключ из заголовка ``X-API-Key``.

        Raises:
            CredentialsInvalid: Ключ не совпал с ключом сервиса.
        """
