from unittest.mock import patch

import pytest

from payment_service.adapters import database


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
def _settings(**overrides: object) -> database.Settings:
    values: dict[str, object] = {
        "POSTGRES_USER": "payments",
        "POSTGRES_PASSWORD": "p%ss",
        "POSTGRES_DB": "payments",
        **overrides,
    }
    return database.Settings.model_validate(values)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettings:
    def test__database_url(self) -> None:
        # Call
        settings = _settings(POSTGRES_HOST="db", POSTGRES_PORT=5433)

        # Assert
        assert settings.database_url == "postgresql+asyncpg://payments:p%ss@db:5433/payments"

    def test__password_hidden_in_repr(self) -> None:
        # Call and Assert
        assert "p%ss" not in repr(_settings())

    @pytest.mark.parametrize(
        "overrides",
        [{"POSTGRES_PORT": 0}, {"POSTGRES_LOCK_TIMEOUT_MS": 0}, {"POSTGRES_CONNECT_TIMEOUT": 0}],
    )
    def test__bounds(self, overrides: dict[str, object]) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="greater than"):
            _settings(**overrides)


class TestLoggerLevels:
    def test__echo_sql_enables_info(self) -> None:
        # Call and Assert
        assert database.logger_levels(_settings(POSTGRES_ECHO_SQL=True)) == {
            "sqlalchemy.engine": "INFO"
        }

    def test__default_hides_sql(self) -> None:
        # Call and Assert
        assert database.logger_levels(_settings()) == {"sqlalchemy.engine": "WARNING"}


class TestDatabase:
    def test__lock_timeout_passed_to_server_settings(self) -> None:
        # Call
        with patch(f"{database.database.__name__}.create_async_engine") as create_engine:
            database.Database(_settings(POSTGRES_LOCK_TIMEOUT_MS=25_000))

        # Assert: параметр уходит в asyncpg как server setting на каждое соединение
        connect_args = create_engine.call_args.kwargs["connect_args"]
        assert connect_args == {"server_settings": {"lock_timeout": "25000ms"}, "timeout": 10.0}
        assert create_engine.call_args.kwargs["pool_pre_ping"] is True

    def test__no_lock_timeout_by_default(self) -> None:
        # Call
        with patch(f"{database.database.__name__}.create_async_engine") as create_engine:
            database.Database(_settings())

        # Assert
        assert create_engine.call_args.kwargs["connect_args"]["server_settings"] == {}
