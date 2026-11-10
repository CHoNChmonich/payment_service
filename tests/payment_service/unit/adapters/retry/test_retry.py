import logging
from unittest.mock import AsyncMock, call

import pytest

from payment_service.adapters import retry
from payment_service.application import errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
RETRY_LOGGER = "payment_service.adapters.retry.retry"


@pytest.fixture(scope="function")
def sleep() -> AsyncMock:
    return AsyncMock()


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestCall:
    async def test__success_first_time(self, sleep: AsyncMock) -> None:
        # Setup
        fn = AsyncMock(return_value="ok")

        # Call
        result = await retry.Retry(attempts=3, backoff=1).call(
            fn, retry_on=errors.UnavailableError, sleep=sleep
        )

        # Assert
        assert result == "ok"
        assert fn.await_count == 1
        sleep.assert_not_awaited()

    async def test__success_after_retries_with_doubling_pause(
        self, sleep: AsyncMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Setup
        fn = AsyncMock(side_effect=[errors.GatewayUnavailable(), errors.GatewayUnavailable(), "ok"])

        # Call
        with caplog.at_level(logging.WARNING, logger=RETRY_LOGGER):
            result = await retry.Retry(attempts=3, backoff=1).call(
                fn, retry_on=errors.UnavailableError, sleep=sleep, what="шлюз"
            )

        # Assert: паузы 1 с и 2 с; каждый повтор — предупреждение
        assert result == "ok"
        assert fn.await_count == 3
        assert sleep.await_args_list == [call(1), call(2)]
        assert [record.getMessage() for record in caplog.records] == [
            "шлюз: попытка 1 из 3 не удалась (GatewayUnavailable), повтор через 1.0 с",
            "шлюз: попытка 2 из 3 не удалась (GatewayUnavailable), повтор через 2.0 с",
        ]

    async def test__exhausted_raises_last_error(self, sleep: AsyncMock) -> None:
        # Setup
        last = errors.GatewayUnavailable()
        fn = AsyncMock(side_effect=[errors.GatewayUnavailable(), errors.GatewayUnavailable(), last])

        # Call and Assert
        with pytest.raises(errors.GatewayUnavailable) as excinfo:
            await retry.Retry(attempts=3, backoff=1).call(
                fn, retry_on=errors.UnavailableError, sleep=sleep
            )

        assert excinfo.value is last
        assert fn.await_count == 3
        assert sleep.await_count == 2

    async def test__other_error_not_retried(self, sleep: AsyncMock) -> None:
        # Setup
        fn = AsyncMock(side_effect=errors.WebhookRejected(url="http://h", status_code=400))

        # Call and Assert
        with pytest.raises(errors.WebhookRejected):
            await retry.Retry(attempts=3, backoff=1).call(
                fn, retry_on=errors.UnavailableError, sleep=sleep
            )

        assert fn.await_count == 1
        sleep.assert_not_awaited()

    async def test__single_attempt(self, sleep: AsyncMock) -> None:
        # Setup
        fn = AsyncMock(side_effect=errors.GatewayUnavailable())

        # Call and Assert
        with pytest.raises(errors.GatewayUnavailable):
            await retry.Retry(attempts=1, backoff=1).call(
                fn, retry_on=errors.UnavailableError, sleep=sleep
            )

        sleep.assert_not_awaited()

    async def test__backoff_capped(self, sleep: AsyncMock) -> None:
        # Setup
        fn = AsyncMock(side_effect=[errors.GatewayUnavailable()] * 4 + ["ok"])

        # Call
        await retry.Retry(attempts=5, backoff=1, backoff_max=3).call(
            fn, retry_on=errors.UnavailableError, sleep=sleep
        )

        # Assert
        assert sleep.await_args_list == [call(1), call(2), call(3), call(3)]


class TestParameters:
    def test__total_backoff(self) -> None:
        # Call and Assert
        assert retry.Retry(attempts=3, backoff=1).total_backoff == 3
        assert retry.Retry(attempts=1, backoff=1).total_backoff == 0
        assert retry.Retry(attempts=4, backoff=1, backoff_max=2).total_backoff == 5

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"attempts": 0, "backoff": 1}, "Число попыток"),
            ({"attempts": 3, "backoff": -1}, "Пауза не может"),
            ({"attempts": 3, "backoff": 2, "backoff_max": 1}, "Потолок паузы"),
        ],
    )
    def test__invalid(self, kwargs: dict[str, float], message: str) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match=message):
            retry.Retry(**kwargs)  # type: ignore[arg-type]
