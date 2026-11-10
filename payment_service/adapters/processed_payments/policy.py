"""Политика ошибок обработки сообщения: исключение → действие консьюмера, код причины для DLQ.

Единственное место решения, как ``api.error_handlers`` для HTTP: по категориям ошибок ядра, а
не по конкретным классам — новая ошибка ядра не требует правок адаптера. Как и у HTTP, «плохой
вход» — только ошибка разбора на границе; ошибка ядра без категории и любое другое исключение
— дефект (у HTTP — 500, здесь — остановка). Таблица та же, что у ``new_payments``:
совпадение держит общий тест политик.
"""

from payment_service.adapters.rabbitmq import Action
from payment_service.application import errors

from .handler import InvalidMessage


def action_for(error: BaseException) -> Action:
    """Выбирает действие по исключению попытки обработки.

    - ``InvalidMessage`` и ошибки ядра категорий ``NotFoundError``, ``ConflictError``,
      ``RuleViolationError`` (в том числе ``WebhookRejected``, ``WebhookAddressInvalid``),
      ``AuthenticationError`` — ``DEAD_LETTER``: повтор того же сообщения даст тот же отказ;
    - ``InfrastructureUnavailable`` (БД, брокер) и ``TimeoutError`` дедлайна попытки —
      ``RETRY``: в DLQ из-за собственной инфраструктуры не уходит, иначе при лежащей БД вся
      очередь слилась бы в DLQ;
    - прочие ``UnavailableError`` — внешняя сторона отказала и после повторов декоратора
      (``WebhookUnavailable``) — ``DEAD_LETTER``: это и есть «не обработано после трёх попыток»;
    - прочее (ошибка ядра без категории, ``ValidationError`` из сценария, дефект, схема или
      права БД) — ``STOP``: процесс падает заметно, сообщение будет обработано после исправления.

    Args:
        error: Исключение попытки обработки.

    Returns:
        Действие консьюмера.
    """
    match error:
        case InvalidMessage():
            return Action.DEAD_LETTER
        case errors.InfrastructureUnavailable() | TimeoutError():
            return Action.RETRY
        case errors.UnavailableError():
            return Action.DEAD_LETTER
        case (
            errors.NotFoundError()
            | errors.ConflictError()
            | errors.RuleViolationError()
            | errors.AuthenticationError()
        ):
            return Action.DEAD_LETTER
        case _:
            return Action.STOP


def error_code(error: BaseException) -> str:
    """Код причины отказа для заголовка DLQ: код ошибки ядра или имя класса исключения.

    Args:
        error: Причина отказа.

    Returns:
        Машинный код причины.
    """
    return error.code if isinstance(error, errors.Error) else type(error).__name__
