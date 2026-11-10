"""Шов настроек аутентификации и разбора заголовка ``X-API-Key``.

Границы ключа у ``auth.Settings`` и у контроллера — свои у каждого адаптера; ключ, который
приняли настройки, обязан проходить разбор заголовка, иначе сервис отвечал бы 401 на каждый
запрос.
"""

from unittest.mock import Mock, call

import pytest

from payment_service.adapters import auth
from payment_service.adapters.api.v1.controller import Controller
from payment_service.application import interfaces

# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
# Все видимые символы ASCII — крайние допустимые значения синтаксиса ключа
PRINTABLE_ASCII = "".join(chr(code) for code in range(0x21, 0x7F))


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettingsKeyPassesHeaderParsing:
    @pytest.mark.parametrize(
        "api_key",
        [
            "a" * auth.MIN_API_KEY_LENGTH,
            "b" * auth.MAX_API_KEY_LENGTH,
            PRINTABLE_ASCII,
        ],
    )
    async def test__accepted_key_reaches_authenticator(self, api_key: str) -> None:
        # Setup: ключ принят настройками
        settings = auth.Settings.model_validate({"AUTH_API_KEY": api_key})
        authenticator = Mock(interfaces.Authenticator)
        controller = Controller(authenticator=authenticator, payment=Mock(interfaces.Payment))

        # Call
        await controller._authenticate(settings.api_key)

        # Assert: заголовок разобран и ключ дошёл до сверки
        assert authenticator.method_calls == [call.authenticate(api_key)]
