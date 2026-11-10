"""Шов HTTP ↔ ядро: тело, прошедшее ``PaymentCreateBody``, принимается DTO ядра.

Ограничения полей у ``*Body`` и ``dtos.NewPaymentInfo`` — общие типы ``dtos.base``; тесты
держат это правило на граничных входах, чтобы расхождение не превратилось в 500 на
``to_application()``.
"""

from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from payment_service.adapters.api.v1 import dtos as api_dtos
from payment_service.adapters.api.v1 import schemas as api_schemas
from payment_service.application import dtos, entities

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
MAX_KEY = "k" * dtos.MAX_IDEMPOTENCY_KEY_LENGTH
BASE: dict[str, Any] = {"amount": "1", "currency": "RUB", "webhook_url": "http://h/x"}


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestPaymentCreateBody:
    @pytest.mark.parametrize(
        "payload",
        [
            {**BASE, "amount": "9999999999999999.99"},
            {**BASE, "amount": "0.01"},
            {**BASE, "description": "д" * dtos.MAX_DESCRIPTION_LENGTH},
            {**BASE, "metadata": {"k": "ы" * 2000}},
            {**BASE, "webhook_url": "https://h/" + "p" * (dtos.MAX_WEBHOOK_URL_LENGTH - 10)},
            {**BASE, "currency": "EUR"},
            # Хосты, которые не A-метки, IDNA не проверяет — как и HTTP-клиент
            {**BASE, "webhook_url": "http://webhook_sink:8080/payments"},
            {**BASE, "webhook_url": "http://[::1]:8080/x"},
            {**BASE, "webhook_url": "http://sub.xn--zz-xyz/x"},
        ],
    )
    def test__edge_inputs_are_accepted_by_core(self, payload: dict[str, Any]) -> None:
        # Call
        info = api_schemas.PaymentCreateBody.model_validate(payload).to_application(MAX_KEY)

        # Assert
        assert isinstance(info, dtos.NewPaymentInfo)
        assert info.idempotency_key == MAX_KEY
        assert isinstance(info.currency, entities.Currency)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("amount", "99999999999999999.99"),
            ("amount", "0.001"),
            ("amount", "0"),
            ("description", "д" * (dtos.MAX_DESCRIPTION_LENGTH + 1)),
            ("metadata", {"k": "ы" * 5000}),
            ("metadata", {"k": "a\x00b"}),
            ("webhook_url", "https://h/" + "p" * dtos.MAX_WEBHOOK_URL_LENGTH),
            ("description", "a\x00b"),
            ("webhook_url", "http://u@h/x"),
            # Битый punycode: HTTP-клиент не раскодирует хост — уведомить было бы некуда
            ("webhook_url", "http://xn--zz-xyz/x"),
            # Вход 1009 символов, после percent-encoding — 6009: колонка не вместит
            ("webhook_url", "http://h/" + "д" * 1000),
            ("currency", "GBP"),
        ],
    )
    def test__rejected_at_the_edge(self, field: str, value: object) -> None:
        # Call and Assert: отвергнуто именно это поле, а не что-то в остальном теле
        with pytest.raises(ValidationError) as excinfo:
            api_schemas.PaymentCreateBody.model_validate({**BASE, field: value})
        assert {error["loc"][0] for error in excinfo.value.errors()} == {field}

    def test__currency_round_trip(self) -> None:
        # Call and Assert
        for currency in entities.Currency:
            edge = api_schemas.Currency.from_application(currency)
            assert edge.to_application() is currency
        assert {c.value for c in api_schemas.Currency} == {c.value for c in entities.Currency}


class TestPaymentStatus:
    def test__covers_core_statuses(self) -> None:
        # Call and Assert
        assert {s.value for s in api_dtos.PaymentStatus} == {
            s.value for s in entities.PaymentStatus
        }
        for status in entities.PaymentStatus:
            assert api_dtos.PaymentStatus.from_application(status).value == status.value


class TestMoney:
    @pytest.mark.parametrize(
        ("amount", "expected"),
        [(Decimal("5"), "5.00"), (Decimal("100.5"), "100.50"), (Decimal("0.1") * 3, "0.30")],
    )
    def test__two_decimal_places(self, amount: Decimal, expected: str) -> None:
        # Setup
        info = dtos.PaymentInfo.model_validate(
            {
                "id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
                "idempotency_key": "k",
                "amount": amount,
                "currency": "RUB",
                "description": "",
                "metadata": {},
                "webhook_url": "http://h/x",
                "status": "pending",
                "created_at": "2026-11-10T12:00:00Z",
                "processed_at": None,
                "notified_at": None,
            }
        )

        # Call
        dumped = api_dtos.PaymentResponse.from_application(info).model_dump(mode="json")

        # Assert
        assert dumped["amount"] == expected
