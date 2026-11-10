"""Общие ограничения полей DTO: границы входа совпадают с границами хранилища.

Те же типы использует HTTP-шов (``adapters/api/v1/schemas``): тело запроса, прошедшее
валидацию на границе, гарантированно принимается ядром, а вход, который хранилище
не вместит, отклоняется как 400, а не падает в 500 на запросе к БД.
"""

import json
from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, AnyUrl, Field, JsonValue, UrlConstraints
from pydantic_core import PydanticCustomError

MAX_DESCRIPTION_LENGTH = 1000
MAX_IDEMPOTENCY_KEY_LENGTH = 255
MAX_WEBHOOK_URL_LENGTH = 2048
MAX_METADATA_BYTES = 4096  # сериализованный JSON метаданных

# Ключ идемпотентности без пробельных символов по краям: иначе « key» и «key» — разные ключи
# для БД, но один для человека.
IDEMPOTENCY_KEY_PATTERN = r"^\S(.*\S)?$"

# API-ключ: видимые символы ASCII без пробелов (RFC 9110 token68 и шире) — иное в заголовке HTTP
# не выживает; длиннее предела — не ключ, а мусор. Общие границы для настроек и HTTP-адаптера:
# ключ, который прошёл настройки, обязан проходить разбор заголовка, иначе 401 на каждый запрос.
MAX_API_KEY_LENGTH = 4096
API_KEY_PATTERN = r"^[\x21-\x7e]+$"

# Сумма платежа: Numeric(18, 2) в хранилище, строго положительная, без NaN и бесконечностей.
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
Description = Annotated[str, Field(max_length=MAX_DESCRIPTION_LENGTH)]
IdempotencyKey = Annotated[
    str,
    Field(min_length=1, max_length=MAX_IDEMPOTENCY_KEY_LENGTH, pattern=IDEMPOTENCY_KEY_PATTERN),
]


def _without_userinfo(url: AnyUrl) -> AnyUrl:
    """Отклоняет URL с учётными данными: они попали бы в ответ API и в логи.

    Args:
        url: Разобранный URL после проверки схемы и длины.

    Returns:
        Тот же URL.

    Raises:
        PydanticCustomError: В URL есть имя пользователя или пароль
            (тип ``webhook_url_userinfo``).
    """
    if url.username is not None or url.password is not None:
        raise PydanticCustomError(
            "webhook_url_userinfo", "URL webhook не должен содержать учётные данные"
        )
    return url


# Адрес webhook: только http/https с хостом, без учётных данных, не длиннее колонки хранилища.
WebhookUrl = Annotated[
    AnyUrl,
    UrlConstraints(
        max_length=MAX_WEBHOOK_URL_LENGTH, allowed_schemes=["http", "https"], host_required=True
    ),
    AfterValidator(_without_userinfo),
]


def _storable_json(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    r"""Проверяет, что метаданные помещаются в JSONB и не длиннее предела.

    PostgreSQL отвергает ``NaN``/``Infinity`` и символ ``\u0000`` внутри JSONB: без проверки
    здесь такой вход упал бы в 500 на запросе к БД.

    Args:
        value: Метаданные после проверки типов.

    Returns:
        Те же метаданные.

    Raises:
        PydanticCustomError: Значение не сериализуется в JSON для хранилища
            (``metadata_not_storable``) или длиннее ``MAX_METADATA_BYTES`` (``metadata_too_large``).
    """
    try:
        dumped = json.dumps(value, ensure_ascii=False, allow_nan=False)
    except ValueError as exc:
        raise PydanticCustomError(
            "metadata_not_storable", "Метаданные содержат значения, недопустимые в JSON"
        ) from exc
    if _contains_nul(value):
        raise PydanticCustomError(
            "metadata_not_storable", "Метаданные содержат недопустимый символ \\u0000"
        )
    if len(dumped.encode()) > MAX_METADATA_BYTES:
        raise PydanticCustomError(
            "metadata_too_large",
            "Метаданные длиннее {limit} байт в JSON",
            {"limit": MAX_METADATA_BYTES},
        )
    return value


def _contains_nul(value: JsonValue) -> bool:
    r"""Есть ли символ ``\u0000`` в строках значения, включая ключи вложенных словарей.

    ``json.dumps`` экранирует его в ``\u0000``, поэтому искать в сериализованном тексте
    нельзя: литеральная строка ``\u0000`` клиента выглядела бы так же.

    Args:
        value: Значение JSON любой вложенности.

    Returns:
        ``True``, если символ встречается хотя бы в одной строке.
    """
    match value:
        case str():
            return "\x00" in value
        case dict():
            return any("\x00" in key or _contains_nul(item) for key, item in value.items())
        case list():
            return any(_contains_nul(item) for item in value)
        case _:
            return False


Metadata = Annotated[dict[str, JsonValue], AfterValidator(_storable_json)]
