import pytest
from pydantic import ValidationError

from payment_service.adapters import auth
from payment_service.application import errors

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
API_KEY = "s3cret-api-key-0123456789"
SETTINGS = auth.Settings.model_validate({"AUTH_API_KEY": API_KEY})


@pytest.fixture(scope="function")
def authenticator() -> auth.Authenticator:
    return auth.Authenticator(SETTINGS)


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestAuthenticate:
    async def test__valid_key(self, authenticator: auth.Authenticator) -> None:
        # Call and Assert: без исключения
        await authenticator.authenticate(API_KEY)

    @pytest.mark.parametrize(
        "api_key",
        [
            "",
            "S3CRET-API-KEY-0123456789",
            API_KEY[:-1],
            API_KEY + "0",
            " " + API_KEY,
            "ключ-с-не-ascii-символами",
        ],
    )
    async def test__invalid_key(self, authenticator: auth.Authenticator, api_key: str) -> None:
        # Call and Assert
        with pytest.raises(errors.CredentialsInvalid):
            await authenticator.authenticate(api_key)


class TestSettings:
    def test__secret_hidden(self) -> None:
        # Call and Assert
        assert API_KEY not in repr(SETTINGS)
        assert API_KEY not in str(SETTINGS)
        assert SETTINGS.api_key == API_KEY

    def test__required(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Setup: ни окружения, ни .env
        monkeypatch.delenv("AUTH_API_KEY", raising=False)

        # Call and Assert
        with pytest.raises(ValidationError):
            auth.Settings(_env_file=None)

    @pytest.mark.parametrize(
        "api_key",
        ["ключ-с-кириллицей-0123456789", "key with spaces 0123", " key-0123456789abcdef"],
    )
    def test__non_printable_ascii_rejected(self, api_key: str) -> None:
        # Call and Assert: контроллер отбросил бы такой ключ до сверки (всегда 401); ключ скрыт
        with pytest.raises(ValidationError) as excinfo:
            auth.Settings.model_validate({"AUTH_API_KEY": api_key})
        assert api_key not in str(excinfo.value)

    @pytest.mark.parametrize(
        "api_key", ["a" * (auth.MIN_API_KEY_LENGTH - 1), "b" * (auth.MAX_API_KEY_LENGTH + 1)]
    )
    def test__length_out_of_bounds(self, api_key: str) -> None:
        # Call and Assert: ключ не попадает в текст ошибки
        with pytest.raises(ValidationError) as excinfo:
            auth.Settings.model_validate({"AUTH_API_KEY": api_key})
        assert api_key not in str(excinfo.value)

    def test__min_length_accepted(self) -> None:
        # Call
        settings = auth.Settings.model_validate({"AUTH_API_KEY": "a" * auth.MIN_API_KEY_LENGTH})

        # Assert
        assert settings.api_key == "a" * auth.MIN_API_KEY_LENGTH
