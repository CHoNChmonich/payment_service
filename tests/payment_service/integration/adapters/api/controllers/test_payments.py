from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import Mock, call
from uuid import UUID

import httpx
import pytest

from payment_service.application import dtos, entities, errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
PAYMENT_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")
CREATED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
WEBHOOK_URL = "https://shop.example.com/hooks/payments"
VALID_BODY: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Заказ №42",
    "metadata": {"order_id": 42, "tags": ["a"]},
    "webhook_url": WEBHOOK_URL,
}
IDEMPOTENCY_HEADERS = {"Idempotency-Key": "order-42-attempt-1"}


def _payment_info(
    status: entities.PaymentStatus = entities.PaymentStatus.PENDING, **overrides: object
) -> dtos.PaymentInfo:
    values: dict[str, Any] = {
        "id": PAYMENT_ID,
        "idempotency_key": "order-42-attempt-1",
        "amount": Decimal("100.50"),
        "currency": entities.Currency.RUB,
        "description": "Заказ №42",
        "metadata": {"order_id": 42, "tags": ["a"]},
        "webhook_url": WEBHOOK_URL,
        "status": status,
        "created_at": CREATED_AT,
        "processed_at": None,
        "notified_at": None,
        **overrides,
    }
    return dtos.PaymentInfo.model_validate(values)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCreatePayment:
    async def test__accepted(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Setup
        payment_service.create.return_value = _payment_info()

        # Call
        response = await client.post(
            "/api/v1/payments", json=VALID_BODY, headers=IDEMPOTENCY_HEADERS
        )

        # Assert: 202 с тремя полями; в ядро ушёл DTO с ключом из заголовка
        assert response.status_code == 202
        assert response.json() == {
            "payment_id": str(PAYMENT_ID),
            "status": "pending",
            "created_at": "2026-11-10T12:00:00Z",
        }
        expected = dtos.NewPaymentInfo.model_validate(
            {"idempotency_key": "order-42-attempt-1", **VALID_BODY}
        )
        assert payment_service.method_calls == [call.create(expected)]

    async def test__amount_as_number(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Setup
        payment_service.create.return_value = _payment_info()

        # Call
        response = await client.post(
            "/api/v1/payments", json={**VALID_BODY, "amount": 100.5}, headers=IDEMPOTENCY_HEADERS
        )

        # Assert
        assert response.status_code == 202
        assert payment_service.create.call_args.args[0].amount == Decimal("100.5")

    async def test__minimal_body(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Setup
        payment_service.create.return_value = _payment_info()

        # Call
        response = await client.post(
            "/api/v1/payments",
            json={"amount": "1", "currency": "EUR", "webhook_url": WEBHOOK_URL},
            headers=IDEMPOTENCY_HEADERS,
        )

        # Assert
        assert response.status_code == 202
        info = payment_service.create.call_args.args[0]
        assert (info.description, info.metadata) == ("", {})

    async def test__status_from_service_in_202(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Setup: контроллер повторов не различает — 202 со статусом, который вернул сценарий
        # (так выглядит повтор ключа после обработки)
        payment_service.create.return_value = _payment_info(
            status=entities.PaymentStatus.SUCCEEDED, processed_at=CREATED_AT
        )

        # Call
        response = await client.post(
            "/api/v1/payments", json=VALID_BODY, headers=IDEMPOTENCY_HEADERS
        )

        # Assert
        assert response.status_code == 202
        assert response.json()["status"] == "succeeded"

    async def test__idempotency_key_conflict(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Setup
        payment_service.create.side_effect = errors.IdempotencyKeyConflict(
            idempotency_key="order-42-attempt-1"
        )

        # Call
        response = await client.post(
            "/api/v1/payments", json=VALID_BODY, headers=IDEMPOTENCY_HEADERS
        )

        # Assert
        assert response.status_code == 409
        assert response.json() == {
            "error": "Платёж с Idempotency-Key order-42-attempt-1 уже создан с другими данными",
            "code": "idempotency_key_conflict",
            "context": {"idempotency_key": "order-42-attempt-1"},
        }

    async def test__idempotency_key_missing(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Call
        response = await client.post("/api/v1/payments", json=VALID_BODY)

        # Assert
        assert response.status_code == 400
        body = response.json()
        assert body["code"] == "validation_error"
        assert [tuple(detail["loc"]) for detail in body["context"]["details"]] == [
            ("header", "Idempotency-Key")
        ]
        payment_service.create.assert_not_called()

    @pytest.mark.parametrize("key", ["", " ", "a" * 256])
    async def test__idempotency_key_invalid(
        self, key: str, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Call
        response = await client.post(
            "/api/v1/payments", json=VALID_BODY, headers={"Idempotency-Key": key}
        )

        # Assert
        assert response.status_code == 400
        assert response.json()["code"] == "validation_error"
        payment_service.create.assert_not_called()

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("amount", "0"),
            ("amount", "-1"),
            ("amount", "1.005"),
            ("amount", "abc"),
            ("currency", "GBP"),
            ("currency", "rub"),
            ("description", "x" * 1001),
            ("metadata", []),
            ("metadata", {"k": "x" * 5000}),
            ("webhook_url", "ftp://shop.example.com/hook"),
            ("webhook_url", "https://user:pw@shop.example.com/hook"),
            ("webhook_url", "not a url"),
        ],
    )
    async def test__invalid_field(
        self, field: str, value: object, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Call
        response = await client.post(
            "/api/v1/payments", json={**VALID_BODY, field: value}, headers=IDEMPOTENCY_HEADERS
        )

        # Assert
        assert response.status_code == 400
        body = response.json()
        assert body["code"] == "validation_error"
        assert body["error"] == "Некорректный запрос"
        assert {tuple(detail["loc"]) for detail in body["context"]["details"]} == {("body", field)}
        payment_service.create.assert_not_called()

    @pytest.mark.parametrize("field", ["amount", "currency", "webhook_url"])
    async def test__required_field_missing(
        self, field: str, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Call
        response = await client.post(
            "/api/v1/payments",
            json={k: v for k, v in VALID_BODY.items() if k != field},
            headers=IDEMPOTENCY_HEADERS,
        )

        # Assert
        assert response.status_code == 400
        details = response.json()["context"]["details"]
        assert {tuple(detail["loc"]) for detail in details} == {("body", field)}
        payment_service.create.assert_not_called()

    async def test__unknown_field(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Call
        response = await client.post(
            "/api/v1/payments",
            json={**VALID_BODY, "status": "succeeded"},
            headers=IDEMPOTENCY_HEADERS,
        )

        # Assert
        assert response.status_code == 400
        details = response.json()["context"]["details"]
        assert {tuple(detail["loc"]) for detail in details} == {("body", "status")}
        payment_service.create.assert_not_called()

    async def test__empty_body(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Call
        response = await client.post("/api/v1/payments", content=b"", headers=IDEMPOTENCY_HEADERS)

        # Assert
        assert response.status_code == 400
        assert response.json()["code"] == "validation_error"
        payment_service.create.assert_not_called()


class TestGetPayment:
    async def test__found(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Setup
        payment_service.get.return_value = _payment_info(
            status=entities.PaymentStatus.SUCCEEDED,
            processed_at=datetime(2026, 11, 10, 12, 0, 4, tzinfo=UTC),
            notified_at=datetime(2026, 11, 10, 12, 0, 5, tzinfo=UTC),
        )

        # Call
        response = await client.get(f"/api/v1/payments/{PAYMENT_ID}")

        # Assert
        assert response.status_code == 200
        assert response.json() == {
            "payment_id": str(PAYMENT_ID),
            "idempotency_key": "order-42-attempt-1",
            "amount": "100.50",
            "currency": "RUB",
            "description": "Заказ №42",
            "metadata": {"order_id": 42, "tags": ["a"]},
            "webhook_url": WEBHOOK_URL,
            "status": "succeeded",
            "created_at": "2026-11-10T12:00:00Z",
            "processed_at": "2026-11-10T12:00:04Z",
            "notified_at": "2026-11-10T12:00:05Z",
        }
        assert payment_service.method_calls == [call.get(PAYMENT_ID)]

    async def test__pending_has_null_timestamps(
        self, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Setup
        payment_service.get.return_value = _payment_info()

        # Call
        response = await client.get(f"/api/v1/payments/{PAYMENT_ID}")

        # Assert: формат суммы — TestMoney в unit/adapters/api/v1/test_schemas.py
        body = response.json()
        assert (body["status"], body["processed_at"], body["notified_at"]) == (
            "pending",
            None,
            None,
        )

    async def test__not_found(self, payment_service: Mock, client: httpx.AsyncClient) -> None:
        # Setup
        payment_service.get.side_effect = errors.PaymentNotFound(id=PAYMENT_ID)

        # Call
        response = await client.get(f"/api/v1/payments/{PAYMENT_ID}")

        # Assert
        assert response.status_code == 404
        assert response.json() == {
            "error": f"Платёж {PAYMENT_ID} не найден",
            "code": "payment_not_found",
            "context": {"id": str(PAYMENT_ID)},
        }

    @pytest.mark.parametrize("bad_id", ["1", "not-a-uuid", "7c9e6679-7425-40de-944b"])
    async def test__invalid_uuid(
        self, bad_id: str, payment_service: Mock, client: httpx.AsyncClient
    ) -> None:
        # Call
        response = await client.get(f"/api/v1/payments/{bad_id}")

        # Assert
        assert response.status_code == 400
        assert response.json()["code"] == "validation_error"
        payment_service.get.assert_not_called()
