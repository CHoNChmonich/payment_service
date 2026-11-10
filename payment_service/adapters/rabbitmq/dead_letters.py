"""Dead Letter Queue: исходное сообщение и причина отказа в очередь недоставленных."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from aio_pika.abc import AbstractIncomingMessage, HeadersType
from faststream.rabbit import RabbitBroker, RabbitMessage

from .errors import Confirmation, confirmation_of, is_temporary
from .topology import QueueTopology

type ErrorCode = Callable[[BaseException], str]
"""Машинный код причины отказа для заголовка ``x-error-code``; задаёт политика адаптера."""

ERROR_CODE_HEADER = "x-error-code"
ERROR_HEADER = "x-error"
ATTEMPTS_HEADER = "x-attempts"
ORIGINAL_EXCHANGE_HEADER = "x-original-exchange"
ORIGINAL_ROUTING_KEY_HEADER = "x-original-routing-key"
FAILED_AT_HEADER = "x-failed-at"
REASON_HEADERS = frozenset(
    {
        ERROR_CODE_HEADER,
        ERROR_HEADER,
        ATTEMPTS_HEADER,
        ORIGINAL_EXCHANGE_HEADER,
        ORIGINAL_ROUTING_KEY_HEADER,
        FAILED_AT_HEADER,
    }
)
MAX_ERROR_TEXT = 2000


class DeadLetterUnavailable(Exception):
    """Запись в DLQ не подтверждена по временной причине — повторяется только запись."""


class DeadLetterFailed(RuntimeError):
    """Постоянный сбой записи в DLQ (права, отказ брокера) — повтор не поможет."""


@dataclass(frozen=True)
class DeadLetterTarget:
    """Куда и с каким дедлайном писать недоставленные сообщения.

    Attributes:
        topology: Топология консьюмера: DLX и DLQ.
        publish_timeout: Дедлайн подтверждения записи в DLQ в секундах.
    """

    topology: QueueTopology
    publish_timeout: float


class DeadLetters:
    """Публикатор DLQ через брокер процесса.

    В DLQ уходят исходные байты тела, ``message_id`` и исходные заголовки — после исправления
    сообщение можно вернуть в рабочую очередь как есть; причина — в заголовках ``x-error*``,
    ``x-attempts``, ``x-original-*``, ``x-failed-at`` (прежние заголовки причины сообщения, уже
    побывавшего в DLQ, заменяются). Публикация идёт явно в DLX, а не через ``reject`` без
    requeue: так в сообщении есть причина, а брокеру не нужна политика dead-lettering на
    отвергнутые — она остаётся для сообщений, которые RabbitMQ отвергает сам.
    """

    def __init__(
        self, target: DeadLetterTarget, broker: RabbitBroker, error_code: ErrorCode
    ) -> None:
        """Принимает DLQ, брокер процесса (запуск и остановка — у консьюмера) и коды причин.

        Args:
            target: DLX, DLQ и дедлайн записи.
            broker: Брокер FastStream, общий с консьюмером.
            error_code: Код причины отказа; библиотека не знает ошибок ядра — код даёт адаптер.
        """
        self._target = target
        self._broker = broker
        self._error_code = error_code
        self._exchange = target.topology.dead_letter_exchange_object(declare=False)

    async def publish(self, message: RabbitMessage, error: BaseException, attempts: int) -> None:
        """Пишет сообщение в DLQ и ждёт подтверждения записи.

        Args:
            message: Исходное сообщение.
            error: Причина отказа.
            attempts: Сколько попыток обработки было сделано.

        Raises:
            DeadLetterUnavailable: Запись не подтверждена по временной причине.
            DeadLetterFailed: Постоянный сбой записи.
        """
        raw = message.raw_message
        try:
            confirmation = await self._broker.publish(
                raw.body,
                exchange=self._exchange,
                routing_key=self._target.topology.dead_letter_queue,
                mandatory=True,
                persist=True,
                message_id=raw.message_id,
                content_type=raw.content_type,
                headers={
                    **_original_headers(raw.headers),
                    **self._reason_headers(error, attempts, raw),
                },
                timeout=self._target.publish_timeout,
            )
        except Exception as exc:
            if is_temporary(exc):
                raise DeadLetterUnavailable(f"DLQ недоступна: {exc}") from exc
            raise DeadLetterFailed(f"Запись в DLQ невозможна: {exc}") from exc
        match confirmation_of(confirmation):
            case Confirmation.ACKED:
                return
            case Confirmation.RETURNED:
                raise DeadLetterUnavailable("DLQ недоступна: сообщение возвращено, очереди нет")
            case Confirmation.REJECTED:
                raise DeadLetterFailed("Запись в DLQ невозможна: брокер отверг сообщение")

    def _reason_headers(
        self, error: BaseException, attempts: int, raw: AbstractIncomingMessage
    ) -> dict[str, str | int]:
        """Заголовки причины: кто и почему отверг сообщение, откуда оно.

        Args:
            error: Причина отказа.
            attempts: Число попыток обработки.
            raw: Исходное сообщение aio-pika (обменник и ключ маршрутизации).

        Returns:
            Имя заголовка → значение.
        """
        return {
            ERROR_CODE_HEADER: self._error_code(error),
            ERROR_HEADER: str(error)[:MAX_ERROR_TEXT],
            ATTEMPTS_HEADER: attempts,
            ORIGINAL_EXCHANGE_HEADER: raw.exchange or "",
            ORIGINAL_ROUTING_KEY_HEADER: raw.routing_key or "",
            FAILED_AT_HEADER: datetime.now(UTC).isoformat(),
        }


def _original_headers(headers: HeadersType | None) -> HeadersType:
    """Исходные заголовки сообщения без прежних заголовков причины.

    Args:
        headers: Заголовки исходного сообщения.

    Returns:
        Заголовки, которые переносятся в DLQ как есть.
    """
    return {name: value for name, value in (headers or {}).items() if name not in REASON_HEADERS}
