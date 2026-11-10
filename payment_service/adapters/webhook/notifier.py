"""Доставка webhook по HTTP (httpx) с переводом исхода в ошибки ядра."""

import asyncio
import http.cookiejar
import logging

import httpx

from payment_service.application import dtos, errors, interfaces

from .messages import CONTENT_TYPE, PAYMENT_ID_HEADER, WebhookMessage
from .settings import Settings

logger = logging.getLogger(__name__)

# 4xx, которые означают не отказ получателя, а его перегрузку или медлительность — временно.
_TEMPORARY_CLIENT_ERRORS = frozenset({408, 429})


def logger_levels() -> dict[str, str]:
    """Уровни логгеров httpx для конфигурации логирования процесса.

    Клиент пишет строку на каждый запрос уровнем ``INFO``; доставку webhook адаптер логирует сам.

    Returns:
        Имя логгера → уровень.
    """
    return {"httpx": "WARNING", "httpcore": "WARNING"}


class WebhookNotifier(interfaces.WebhookNotifier):
    """Реализация порта уведомлений: ``POST`` тела ``WebhookMessage`` на ``webhook_url``.

    Клиент один на процесс и общий для всех получателей, поэтому он не хранит cookies
    (``Set-Cookie`` одного получателя иначе ушёл бы другому), не читает окружение
    (``HTTP_PROXY``: уведомление не должно уйти через прокси, о котором сервис не знает) и не
    следует редиректам (адрес задал клиент; ответ 3xx — временная недоступность, не доставка).
    Повторы — дело декоратора ``RetryingWebhookNotifier``. Клиент одноразовый: после
    ``aclose`` запросы невозможны.
    """

    def __init__(
        self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        """Создаёт HTTP-клиент; соединения открываются лениво.

        Args:
            settings: Дедлайн доставки.
            transport: Транспорт httpx; по умолчанию сетевой транспорт библиотеки, в тестах —
                ``MockTransport``.
        """
        self._timeout = settings.WEBHOOK_TIMEOUT
        self._client = httpx.AsyncClient(
            timeout=settings.WEBHOOK_TIMEOUT,
            transport=transport,
            trust_env=False,
            follow_redirects=False,
        )
        self._client.cookies.jar.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))

    async def notify(self, payment: dtos.PaymentInfo) -> None:
        """Отправляет уведомление и трактует ответ получателя.

        ``asyncio.timeout`` — общий дедлайн запроса вместе с чтением тела: таймаут httpx
        пофазный и сбрасывается на каждом полученном фрагменте. 2xx — доставлено. 4xx, кроме
        408 и 429, — получатель отверг уведомление: повтор даст то же. Всё остальное (3xx без
        перехода, 408, 429, 5xx, сбой транспорта, дедлайн) — получатель недоступен, исходная
        причина в ``__cause__``.

        Args:
            payment: Обработанный платёж.

        Raises:
            WebhookAddressInvalid: Клиент не может построить запрос по адресу (битый хост).
            WebhookRejected: Получатель ответил 4xx (кроме 408 и 429).
            WebhookUnavailable: Получатель недоступен или ответил не 2xx и не 4xx.
        """
        url = payment.webhook_url
        body = WebhookMessage.from_application(payment).body()
        try:
            request = self._client.build_request(
                "POST",
                url,
                content=body,
                headers={"Content-Type": CONTENT_TYPE, PAYMENT_ID_HEADER: str(payment.id)},
            )
        except (httpx.InvalidURL, UnicodeError) as exc:
            raise errors.WebhookAddressInvalid(url=url) from exc
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.send(request)
        except (httpx.HTTPError, TimeoutError) as exc:
            raise errors.WebhookUnavailable(url=url) from exc
        status = response.status_code
        if 200 <= status < 300:
            logger.info("Webhook по платежу %s доставлен: HTTP %s", payment.id, status)
            return
        if 400 <= status < 500 and status not in _TEMPORARY_CLIENT_ERRORS:
            raise errors.WebhookRejected(url=url, status_code=status)
        raise errors.WebhookUnavailable(url=url) from httpx.HTTPStatusError(
            f"Получатель webhook ответил {status}", request=response.request, response=response
        )

    async def aclose(self) -> None:
        """Закрывает соединения клиента при остановке процесса."""
        await self._client.aclose()
