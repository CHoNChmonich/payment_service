"""Dead Letter Queue: исходное сообщение и причина отказа в очередь недоставленных."""

from datetime import UTC, datetime

from aio_pika.abc import AbstractIncomingMessage, HeadersType
from faststream.rabbit import RabbitBroker, RabbitMessage

from payment_service.adapters import rabbitmq
from payment_service.application import errors

from .settings import Settings

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


class DeadLetters:
    """Публикатор DLQ через брокер процесса.

    В DLQ уходят исходные байты тела, ``message_id`` и исходные заголовки — после исправления
    сообщение можно вернуть в рабочую очередь как есть; причина — в заголовках ``x-error*``,
    ``x-attempts``, ``x-original-*``, ``x-failed-at`` (прежние заголовки причины сообщения, уже
    побывавшего в DLQ, заменяются). Публикация идёт явно в DLX, а не через ``reject`` без
    requeue: так в сообщении есть причина, а брокеру не нужна политика dead-lettering на
    отвергнутые — она остаётся для сообщений, которые RabbitMQ отвергает сам.
    """

    def __init__(self, settings: Settings, broker: RabbitBroker) -> None:
        """Принимает настройки и брокер процесса (запуск и остановка — у консьюмера).

        Args:
            settings: Имена DLX и DLQ, дедлайн записи.
            broker: Брокер FastStream, общий с консьюмером.
        """
        self._settings = settings
        self._broker = broker
        self._exchange = settings.topology.dead_letter_exchange_object(declare=False)

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
                routing_key=self._settings.NEW_PAYMENTS_DLQ,
                mandatory=True,
                persist=True,
                message_id=raw.message_id,
                content_type=raw.content_type,
                headers={
                    **_original_headers(raw.headers),
                    **self._reason_headers(error, attempts, raw),
                },
                timeout=self._settings.NEW_PAYMENTS_DLQ_PUBLISH_TIMEOUT,
            )
        except Exception as exc:
            if rabbitmq.is_temporary(exc):
                raise DeadLetterUnavailable(f"DLQ недоступна: {exc}") from exc
            raise DeadLetterFailed(f"Запись в DLQ невозможна: {exc}") from exc
        match rabbitmq.confirmation_of(confirmation):
            case rabbitmq.Confirmation.ACKED:
                return
            case rabbitmq.Confirmation.RETURNED:
                raise DeadLetterUnavailable("DLQ недоступна: сообщение возвращено, очереди нет")
            case rabbitmq.Confirmation.REJECTED:
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
        code = error.code if isinstance(error, errors.Error) else type(error).__name__
        return {
            ERROR_CODE_HEADER: code,
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
