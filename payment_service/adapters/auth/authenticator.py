"""Проверка API-ключа сверкой со статическим ключом из настроек."""

import secrets

from payment_service.application import errors, interfaces

from .settings import Settings


class Authenticator(interfaces.Authenticator):
    """Реализация порта ``interfaces.Authenticator`` со статическим ключом.

    Сверка — ``secrets.compare_digest`` по байтам: время сравнения не зависит от того, в каком
    символе ключи разошлись, а строка любого содержимого (в том числе не-ASCII) сравнивается без
    исключений.
    """

    def __init__(self, settings: Settings) -> None:
        """Запоминает ожидаемый ключ.

        Args:
            settings: Настройки с действительным ключом.
        """
        self._expected = settings.api_key.encode("utf-8")

    async def authenticate(self, api_key: str) -> None:
        """Сверяет предъявленный ключ с ожидаемым.

        Args:
            api_key: Ключ из заголовка ``X-API-Key``.

        Raises:
            CredentialsInvalid: Ключ не совпал.
        """
        if not secrets.compare_digest(api_key.encode("utf-8"), self._expected):
            raise errors.CredentialsInvalid()
