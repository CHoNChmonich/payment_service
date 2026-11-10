"""Обработчики HTTP v1: аутентификация → разбор запроса → DTO ядра → сценарий → DTO ответа.

Класс получает входящий порт сценариев (``interfaces.Payment``) и исходящий порт
``interfaces.Authenticator`` через конструктор из composition root — тот же «чистый» DI, что и у
остальных адаптеров; FastAPI-``Depends`` не используется. ``Path``/``Body``/``Header`` ниже —
разбор запроса, а не внедрение зависимостей. Границы заголовка ``Idempotency-Key`` — те же, что у
DTO ядра (``dtos.base``).
"""

import re
from typing import Annotated
from uuid import UUID

from fastapi import Body, Header, Path

from payment_service.adapters.api.v1 import dtos as api_dtos
from payment_service.adapters.api.v1 import schemas as api_schemas
from payment_service.application import dtos as app_dtos
from payment_service.application import errors, interfaces

PaymentIdPath = Annotated[UUID, Path(description="Идентификатор платежа (UUID).")]
IdempotencyKeyHeader = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=app_dtos.MAX_IDEMPOTENCY_KEY_LENGTH,
        pattern=app_dtos.IDEMPOTENCY_KEY_PATTERN,
        description="Ключ идемпотентности запроса на создание; повтор с тем же ключом и телом "
        "возвращает исходный платёж.",
    ),
]
# Скрыт из параметров операций: требование ключа объявляет схема безопасности OpenAPI.
ApiKeyHeader = Annotated[str | None, Header(alias="X-API-Key", include_in_schema=False)]

# Синтаксис ключа — общий с настройками аутентификации (``dtos.base``): мусор до сверки не доходит.
_API_KEY = re.compile(app_dtos.API_KEY_PATTERN)


class Controller:
    """Обработчики API v1.

    Каждый обработчик, кроме ``healthcheck``, первой строкой вызывает ``_authenticate``, поэтому
    любой из них может ответить ``CredentialsMissing`` / ``CredentialsInvalid`` (401); в
    ``Raises`` обработчиков перечислены только ошибки сценария.

    Attributes:
        authenticator: Проверка API-ключа.
        payment: Сценарии платежа.
    """

    def __init__(
        self, *, authenticator: interfaces.Authenticator, payment: interfaces.Payment
    ) -> None:
        """Принимает аутентификатор и порт сценариев из composition root.

        Args:
            authenticator: Проверка API-ключа.
            payment: Сценарии платежа.
        """
        self.authenticator = authenticator
        self.payment = payment

    async def create_payment(
        self,
        body: Annotated[api_schemas.PaymentCreateBody, Body()],
        idempotency_key: IdempotencyKeyHeader,
        x_api_key: ApiKeyHeader = None,
    ) -> api_dtos.PaymentAcceptedResponse:
        """Принимает платёж в обработку; обработка асинхронная, результат — по ``GET``.

        Args:
            body: Сумма, валюта, назначение, метаданные и адрес webhook.
            idempotency_key: Заголовок ``Idempotency-Key``.
            x_api_key: Заголовок ``X-API-Key``.

        Returns:
            Идентификатор, статус и момент создания платежа (202).

        Raises:
            IdempotencyKeyConflict: Под этим ключом уже создан платёж с другими данными (409).
            StorageUnavailable: Временный сбой хранилища (503).
        """
        await self._authenticate(x_api_key)
        result = await self.payment.create(body.to_application(idempotency_key))
        return api_dtos.PaymentAcceptedResponse.from_application(result)

    async def get_payment(
        self, payment_id: PaymentIdPath, x_api_key: ApiKeyHeader = None
    ) -> api_dtos.PaymentResponse:
        """Возвращает платёж по идентификатору.

        Args:
            payment_id: Идентификатор платежа из пути.
            x_api_key: Заголовок ``X-API-Key``.

        Returns:
            Найденный платёж со статусом и временными отметками.

        Raises:
            PaymentNotFound: Платежа нет (404).
            StorageUnavailable: Временный сбой хранилища (503).
        """
        await self._authenticate(x_api_key)
        result = await self.payment.get(payment_id)
        return api_dtos.PaymentResponse.from_application(result)

    async def healthcheck(self) -> dict[str, str]:
        """Служебная проверка живости процесса; без аутентификации, к хранилищу не обращается.

        Returns:
            ``{"status": "ok"}``.
        """
        return {"status": "ok"}

    async def _authenticate(self, x_api_key: str | None) -> None:
        """Проверяет API-ключ — первый шаг каждого защищённого обработчика.

        По краям срезаются только пробелы и табуляции (OWS по RFC 9110); ключ из других
        символов, кроме видимых ASCII, или длиннее ``dtos.MAX_API_KEY_LENGTH`` до сверки не доходит.
        При нескольких заголовках ``X-API-Key`` FastAPI отдаёт первый.

        Args:
            x_api_key: Значение заголовка ``X-API-Key`` либо ``None``.

        Raises:
            CredentialsMissing: Заголовка нет или его значение не похоже на ключ (401).
            CredentialsInvalid: Ключ не совпал с ключом сервиса (401).
        """
        if x_api_key is None:
            raise errors.CredentialsMissing()
        api_key = x_api_key.strip(" \t")
        if not _API_KEY.fullmatch(api_key) or len(api_key) > app_dtos.MAX_API_KEY_LENGTH:
            raise errors.CredentialsMissing()
        await self.authenticator.authenticate(api_key)
