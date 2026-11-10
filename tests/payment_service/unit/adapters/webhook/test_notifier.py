import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

import httpx
import pytest

from payment_service.adapters import retry, webhook
from payment_service.application import dtos, entities, errors, interfaces

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT = dtos.PaymentInfo(
    id=UUID("11111111-1111-1111-1111-111111111111"),
    idempotency_key="key-1",
    amount=Decimal("10.5"),
    currency=entities.Currency.EUR,
    description="заказ 7",
    metadata={"order": 7},
    webhook_url="http://sink:8080/hook",
    status=entities.PaymentStatus.SUCCEEDED,
    created_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    processed_at=datetime(2026, 11, 10, 12, 0, 4, tzinfo=UTC),
    notified_at=None,
)
SETTINGS = webhook.Settings.model_validate({"WEBHOOK_TIMEOUT": 0.2})


def _notifier(
    handler: httpx.MockTransport | None = None, *, status: int = 200
) -> webhook.WebhookNotifier:
    transport = handler or httpx.MockTransport(lambda request: httpx.Response(status))
    return webhook.WebhookNotifier(SETTINGS, transport=transport)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestNotify:
    async def test__posts_json_body_with_payment_header(self) -> None:
        # Setup
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(204)

        notifier = _notifier(httpx.MockTransport(handler))

        # Call
        await notifier.notify(PAYMENT)

        # Assert
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == "http://sink:8080/hook"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["X-Payment-Id"] == "11111111-1111-1111-1111-111111111111"
        assert json.loads(request.content) == {
            "payment_id": "11111111-1111-1111-1111-111111111111",
            "status": "succeeded",
            "amount": "10.50",
            "currency": "EUR",
            "description": "заказ 7",
            "metadata": {"order": 7},
            "created_at": "2026-11-10T12:00:00Z",
            "processed_at": "2026-11-10T12:00:04Z",
        }

    @pytest.mark.parametrize("status", [400, 404, 410, 422])
    async def test__client_error_is_rejection(self, status: int) -> None:
        # Call and Assert
        with pytest.raises(errors.WebhookRejected) as excinfo:
            await _notifier(status=status).notify(PAYMENT)
        assert excinfo.value.context == {"url": "http://sink:8080/hook", "status_code": status}

    @pytest.mark.parametrize("status", [302, 408, 429, 500, 503])
    async def test__other_statuses_are_unavailability(self, status: int) -> None:
        # Call and Assert
        with pytest.raises(errors.WebhookUnavailable) as excinfo:
            await _notifier(status=status).notify(PAYMENT)
        assert isinstance(excinfo.value.__cause__, httpx.HTTPStatusError)
        assert excinfo.value.__cause__.response.status_code == status

    async def test__transport_error_is_unavailability(self) -> None:
        # Setup
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("отказ в соединении", request=request)

        # Call and Assert
        with pytest.raises(errors.WebhookUnavailable) as excinfo:
            await _notifier(httpx.MockTransport(handler)).notify(PAYMENT)
        assert isinstance(excinfo.value.__cause__, httpx.ConnectError)

    async def test__slow_receiver_hits_deadline(self) -> None:
        # Setup: получатель отвечает дольше WEBHOOK_TIMEOUT = 0.2 с
        async def handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(1)
            return httpx.Response(200)

        # Call and Assert
        with pytest.raises(errors.WebhookUnavailable) as excinfo:
            await _notifier(httpx.MockTransport(handler)).notify(PAYMENT)
        assert isinstance(excinfo.value.__cause__, TimeoutError)

    async def test__redirects_are_not_followed(self) -> None:
        # Setup
        hosts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            hosts.append(request.url.host)
            return httpx.Response(302, headers={"Location": "http://evil/hook"})

        # Call and Assert
        with pytest.raises(errors.WebhookUnavailable):
            await _notifier(httpx.MockTransport(handler)).notify(PAYMENT)
        assert hosts == ["sink"]

    async def test__cookies_are_not_stored(self) -> None:
        # Setup
        cookies: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            cookies.append(request.headers.get("Cookie"))
            return httpx.Response(200, headers={"Set-Cookie": "session=abc"})

        notifier = _notifier(httpx.MockTransport(handler))

        # Call
        await notifier.notify(PAYMENT)
        await notifier.notify(PAYMENT)

        # Assert
        assert cookies == [None, None]


class TestAclose:
    async def test__closes_client(self) -> None:
        # Setup
        notifier = _notifier()

        # Call
        await notifier.aclose()

        # Assert
        with pytest.raises(RuntimeError):
            await notifier.notify(PAYMENT)


class TestRetryingWebhookNotifier:
    async def test__retries_unavailability(self) -> None:
        # Setup: паузы и исчерпание попыток — test_retry.py
        inner = Mock(interfaces.WebhookNotifier)
        inner.notify.side_effect = [errors.WebhookUnavailable(url="u"), None]
        notifier = webhook.RetryingWebhookNotifier(inner, retry.Retry(attempts=3, backoff=0))

        # Call
        await notifier.notify(PAYMENT)

        # Assert
        assert inner.notify.await_count == 2

    async def test__rejection_is_not_retried(self) -> None:
        # Setup
        inner = Mock(interfaces.WebhookNotifier)
        inner.notify.side_effect = errors.WebhookRejected(url="u", status_code=404)
        notifier = webhook.RetryingWebhookNotifier(inner, retry.Retry(attempts=3, backoff=0))

        # Call and Assert
        with pytest.raises(errors.WebhookRejected):
            await notifier.notify(PAYMENT)
        assert inner.notify.await_count == 1


class TestSettings:
    def test__worst_case_counts_all_attempts_and_pauses(self) -> None:
        # Setup: 3 попытки по 5 с и паузы 1 + 2 с
        settings = webhook.Settings.model_validate(
            {"WEBHOOK_TIMEOUT": 5, "WEBHOOK_RETRY_ATTEMPTS": 3, "WEBHOOK_RETRY_BACKOFF": 1}
        )

        # Assert
        assert settings.worst_case == 18.0
