"""Общие ограничения полей DTO: границы входа совпадают с границами хранилища.

Те же типы использует HTTP-шов (``adapters/api/v1/schemas``): тело запроса, прошедшее
валидацию на границе, гарантированно принимается ядром, а вход, который хранилище
не вместит, отклоняется как 400, а не падает в 500 на запросе к БД.
"""

import json
from decimal import Decimal
from typing import Annotated

import idna
from pydantic import AfterValidator, AnyUrl, Field, JsonValue, UrlConstraints
from pydantic_core import PydanticCustomError

MAX_DESCRIPTION_LENGTH = 1000
MAX_IDEMPOTENCY_KEY_LENGTH = 255
MAX_WEBHOOK_URL_LENGTH = 2048
MAX_METADATA_BYTES = 4096  # сериализованный JSON метаданных

# Ключ идемпотентности без пробельных символов по краям: иначе « key» и «key» — разные ключи
# для БД, но один для человека.
IDEMPOTENCY_KEY_PATTERN = r"^\S(.*\S)?$"


def _without_nul(value: str) -> str:
    r"""Отклоняет строку с символом ``\u0000``: PostgreSQL не хранит его в текстовых колонках.

    Args:
        value: Строка после проверки длины.

    Returns:
        Та же строка.

    Raises:
        PydanticCustomError: В строке есть ``\u0000`` (тип ``string_not_storable``).
    """
    if "\x00" in value:
        raise PydanticCustomError(
            "string_not_storable", "Строка содержит недопустимый символ \\u0000"
        )
    return value


# Сумма платежа: Numeric(18, 2) в хранилище, строго положительная, без NaN и бесконечностей.
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
Description = Annotated[str, Field(max_length=MAX_DESCRIPTION_LENGTH), AfterValidator(_without_nul)]
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


def _fits_storage(url: AnyUrl) -> AnyUrl:
    """Отклоняет URL, который после нормализации длиннее колонки хранилища.

    ``UrlConstraints.max_length`` меряет вход, а хранится ``str(url)``: percent-encoding
    кириллицы в пути утраивает длину (1009 символов на входе — 6009 в хранилище).

    Args:
        url: Разобранный и нормализованный URL.

    Returns:
        Тот же URL.

    Raises:
        PydanticCustomError: Нормализованный URL длиннее ``MAX_WEBHOOK_URL_LENGTH``
            (тип ``url_too_long``).
    """
    if len(str(url)) > MAX_WEBHOOK_URL_LENGTH:
        raise PydanticCustomError(
            "url_too_long",
            "URL после нормализации длиннее {max_length} символов",
            {"max_length": MAX_WEBHOOK_URL_LENGTH},
        )
    return url


def _decodable_host(url: AnyUrl) -> AnyUrl:
    """Отклоняет хост в punycode (``xn--…``), который не раскодируется по IDNA 2008.

    Инвариант адреса уведомления: хост в punycode — корректная A-метка IDNA 2008, иначе
    платёж принят, а уведомить некуда. Pydantic отдаёт хост уже в ASCII и A-метки не
    проверяет. Прочие ASCII-хосты (``_`` в имени сервиса, IP-адреса, ``localhost``) не
    проверяются; совпадение правила с HTTP-клиентом держит тест шва webhook-адаптера.

    Args:
        url: Разобранный URL.

    Returns:
        Тот же URL.

    Raises:
        PydanticCustomError: Хост не раскодируется (тип ``webhook_url_host``).
    """
    host = url.host or ""
    if host.startswith("xn--"):
        try:
            idna.decode(host)
        except idna.IDNAError as exc:
            raise PydanticCustomError(
                "webhook_url_host", "Хост URL webhook — недопустимый punycode"
            ) from exc
    return url


# Адрес webhook: только http/https с хостом, без учётных данных, с раскодируемым хостом, не
# длиннее колонки хранилища ни на входе, ни после нормализации.
WebhookUrl = Annotated[
    AnyUrl,
    UrlConstraints(
        max_length=MAX_WEBHOOK_URL_LENGTH, allowed_schemes=["http", "https"], host_required=True
    ),
    AfterValidator(_without_userinfo),
    AfterValidator(_decodable_host),
    AfterValidator(_fits_storage),
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
