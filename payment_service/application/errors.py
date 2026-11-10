"""Ошибки приложения.

Иерархия: ``Error`` → категория (``NotFoundError``, ``ConflictError``, ``RuleViolationError``,
``AuthenticationError``, ``UnavailableError``) → конкретная ошибка. Код ошибки выводится из
имени класса (``PaymentNotFound`` → ``payment_not_found``), текст — из ``message_template`` и
контекста, переданного именованными аргументами. Адаптеры отображают на свои действия именно
категории (HTTP-адаптер: 404 / 409 / 400 / 401 / 503; консьюмер: DLQ для ошибок данных,
правил и внешней стороны, повтор в бюджете — только для подкатегории
``InfrastructureUnavailable``), поэтому новая ошибка не требует правок адаптеров.
"""

import re
from typing import ClassVar

_CAMEL_CASE_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


class Error(Exception):
    """Базовая ошибка логики приложения.

    Attributes:
        message_template: Шаблон текста ошибки; подстановки берутся из контекста.
        code: Машинный код ошибки в snake_case; по умолчанию выводится из имени класса.
        context: Именованные значения, с которыми ошибка была поднята (попадают в ответ API).
        message: Готовый текст ошибки для человека.
    """

    message_template: ClassVar[str] = "Ошибка приложения"
    code: ClassVar[str] = "error"

    def __init_subclass__(cls) -> None:
        """Выводит код ошибки из имени класса-наследника, если он не задан явно."""
        super().__init_subclass__()
        if "code" not in cls.__dict__:
            cls.code = _CAMEL_CASE_BOUNDARY.sub("_", cls.__name__).lower()

    def __init__(self, **context: object) -> None:
        """Собирает текст ошибки из шаблона и контекста.

        Args:
            **context: Значения для подстановки в ``message_template``; сохраняются
                в ``context`` и уходят клиенту в теле ответа.
        """
        self.context = context
        self.message = self.message_template.format_map(context)
        super().__init__(self.message)


class NotFoundError(Error):
    """Категория: запрошенная сущность не найдена."""


class ConflictError(Error):
    """Категория: запрос конфликтует с текущим состоянием данных."""


class RuleViolationError(Error):
    """Категория: нарушено бизнес-правило или получатель отверг данные."""


class AuthenticationError(Error):
    """Категория: запрос не аутентифицирован — ключа нет или он отвергнут.

    Контекст у ошибок категории пустой: ключ не попадает ни в ответ, ни в логи.
    """


class UnavailableError(Error):
    """Категория: зависимость временно недоступна; повтор запроса может помочь.

    Исходное исключение зависимости адаптер передаёт в ``__cause__`` (``raise ... from exc``).
    Подкатегория ``InfrastructureUnavailable`` выделяет собственную инфраструктуру сервиса;
    остальные ошибки категории — внешняя сторона.
    """


class InfrastructureUnavailable(UnavailableError):
    """Подкатегория: недоступна собственная инфраструктура сервиса (хранилище, брокер).

    Сообщение, упавшее из-за такой ошибки, нельзя считать необработанным по своей вине:
    при лежащей БД очередь не должна сливаться в DLQ.
    """


class StorageUnavailable(InfrastructureUnavailable):
    """Хранилище временно недоступно или транзакция прервана.

    Обрыв соединения, дедлок, сбой сериализации, превышен ``lock_timeout``.
    """

    message_template = "Хранилище временно недоступно, повторите попытку"


class BrokerUnavailable(InfrastructureUnavailable):
    """Брокер сообщений временно недоступен: доставка не подтверждена в срок или сбой сети."""

    message_template = "Брокер сообщений временно недоступен, повторите попытку"


class GatewayUnavailable(UnavailableError):
    """Платёжный шлюз не принял платёж: отказ или недоступность.

    Ошибка реализации порта ``PaymentGateway`` — окончательный отказ: платёж переводится в
    ``failed``.
    """

    message_template = "Платёжный шлюз недоступен"


class WebhookUnavailable(UnavailableError):
    """Получатель webhook недоступен или перегружен: доставка не состоялась.

    Ошибка реализации порта ``WebhookNotifier`` — окончательный отказ доставки.
    """

    message_template = "Получатель webhook недоступен по адресу {url}"


class WebhookRejected(RuleViolationError):
    """Получатель webhook отверг уведомление: повтор даст тот же отказ."""

    message_template = "Получатель webhook {url} отверг уведомление: HTTP {status_code}"


class WebhookAddressInvalid(RuleViolationError):
    """Адрес webhook непригоден для доставки: HTTP-клиент не может построить запрос.

    Повтор даст тот же отказ; адрес, принятый до ужесточения проверки на входе, — тоже сюда.
    """

    message_template = "Адрес webhook {url} непригоден для доставки"


class PaymentNotFound(NotFoundError):
    """Платёж с указанным ``id`` отсутствует."""

    message_template = "Платёж {id} не найден"


class IdempotencyKeyConflict(ConflictError):
    """Платёж с этим ``Idempotency-Key`` уже создан с другими данными.

    Повтор того же запроса возвращает исходный платёж; тот же ключ с другим телом — ошибка
    клиента: молча вернуть чужой платёж нельзя.
    """

    message_template = "Платёж с Idempotency-Key {idempotency_key} уже создан с другими данными"


class PaymentTransitionNotAllowed(RuleViolationError):
    """Действие недопустимо в текущем статусе платежа."""

    message_template = "Платёж {id} в статусе {status}: действие «{action}» недопустимо"


class PaymentNotProcessed(RuleViolationError):
    """Уведомление о результате запрошено до того, как платёж обработан."""

    message_template = "Платёж {id} ещё не обработан: уведомлять не о чем"


class CredentialsMissing(AuthenticationError):
    """Заголовка ``X-API-Key`` нет или он пуст."""

    message_template = "Требуется API-ключ в заголовке X-API-Key"


class CredentialsInvalid(AuthenticationError):
    """API-ключ не совпал с ключом сервиса."""

    message_template = "API-ключ недействителен"
