"""Настройки эмуляции платёжного шлюза и повторов обращения к нему."""

from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payment_service import ENV_FILE
from payment_service.adapters import retry


class Settings(BaseSettings):
    """Эмуляция шлюза и повторы: переменные окружения и ``.env`` в корне проекта.

    Attributes:
        PAYMENT_GATEWAY_DELAY_MIN: Наименьшая задержка ответа шлюза в секундах.
        PAYMENT_GATEWAY_DELAY_MAX: Наибольшая задержка ответа шлюза в секундах.
        PAYMENT_GATEWAY_FAILURE_RATE: Доля обращений, на которые шлюз отвечает отказом (0..1);
            ``1`` — все платежи ``failed`` (e2e), ``0`` — все ``succeeded``.
        PAYMENT_GATEWAY_RETRY_ATTEMPTS: Сколько всего обращений к шлюзу на один платёж,
            включая первое; после последнего отказа платёж становится ``failed``.
        PAYMENT_GATEWAY_RETRY_BACKOFF: Пауза перед вторым обращением в секундах; дальше
            удваивается.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    PAYMENT_GATEWAY_DELAY_MIN: float = Field(default=2.0, ge=0, le=60)
    PAYMENT_GATEWAY_DELAY_MAX: float = Field(default=5.0, ge=0, le=60)
    PAYMENT_GATEWAY_FAILURE_RATE: float = Field(default=0.1, ge=0, le=1)
    PAYMENT_GATEWAY_RETRY_ATTEMPTS: int = Field(default=3, ge=1, le=10)
    PAYMENT_GATEWAY_RETRY_BACKOFF: float = Field(default=1.0, ge=0, le=60)

    @property
    def retry_policy(self) -> retry.Retry:
        """Политика повторов обращения к шлюзу для ``RetryingPaymentGateway``."""
        return retry.Retry(
            attempts=self.PAYMENT_GATEWAY_RETRY_ATTEMPTS, backoff=self.PAYMENT_GATEWAY_RETRY_BACKOFF
        )

    @property
    def worst_case(self) -> float:
        """Наибольшее время проведения платежа в секундах: все обращения и паузы между ними.

        Столько транзакция ``process`` держит строку платежа под блокировкой.
        """
        attempts = self.PAYMENT_GATEWAY_RETRY_ATTEMPTS
        return attempts * self.PAYMENT_GATEWAY_DELAY_MAX + self.retry_policy.total_backoff

    @model_validator(mode="after")
    def _delay_range(self) -> Self:
        """Нижняя граница задержки не больше верхней.

        Returns:
            Те же настройки.

        Raises:
            ValueError: ``PAYMENT_GATEWAY_DELAY_MIN`` больше ``PAYMENT_GATEWAY_DELAY_MAX``.
        """
        if self.PAYMENT_GATEWAY_DELAY_MIN > self.PAYMENT_GATEWAY_DELAY_MAX:
            raise ValueError("PAYMENT_GATEWAY_DELAY_MIN не больше PAYMENT_GATEWAY_DELAY_MAX")
        return self
