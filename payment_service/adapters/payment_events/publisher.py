"""Публикация событий платежей в RabbitMQ с подтверждением размещения в очереди."""

import logging
from collections.abc import Callable, Sequence
from typing import assert_never

from faststream.rabbit import RabbitBroker

from payment_service.adapters import rabbitmq
from payment_service.application import dtos, errors, interfaces

from .messages import CONTENT_TYPE, PaymentEventMessage
from .settings import Settings

type BrokerFactory = Callable[[str], RabbitBroker]

logger = logging.getLogger(__name__)


class EventPublishingFailed(RuntimeError):
    """Постоянный сбой публикации: повтор не поможет (права, отказ брокера принять сообщение).

    Не ошибка ядра: это дефект конфигурации или интеграции — процесс должен остановиться
    заметно, а не повторять одно и то же. Одно такое событие блокирует публикацию, пока его
    строку не уберут из outbox вручную; платёж и событие названы в тексте.
    """


class EventPublisher(interfaces.EventPublisher):
    """Публикатор событий платежей через брокер FastStream.

    Ключ маршрутизации — по типу события: «платёж создан» — в очередь новых платежей, «платёж
    проведён» — в очередь уведомлений. Публикация идёт в обменник с ``mandatory=True`` и
    подтверждением: сообщение, которое брокер не смог положить ни в одну очередь (консьюмер ещё
    не объявил свою), возвращается и считается неопубликованным — событие остаётся в outbox до
    следующего цикла relay. Брокер создаётся в конструкторе, но подключается в ``aopen``:
    composition root импортируется и в тестах.
    """

    def __init__(self, settings: Settings, broker_factory: BrokerFactory | None = None) -> None:
        """Создаёт брокер без подключения.

        Args:
            settings: Адрес брокера, обменник, ключ маршрутизации и дедлайн публикации.
            broker_factory: Фабрика брокера по URL; по умолчанию — ``rabbitmq.create_broker``
                с ``graceful_timeout`` под дедлайн публикации; в тестах — фейк.
        """
        self._settings = settings
        factory = broker_factory or self._default_broker
        self._broker = factory(settings.RABBITMQ_URL.get_secret_value())
        self._exchange = rabbitmq.direct_exchange(settings.PAYMENT_EVENTS_EXCHANGE)
        self._opened = False

    def _default_broker(self, url: str) -> RabbitBroker:
        """Брокер проекта с ``graceful_timeout`` не меньше дедлайна публикации.

        Args:
            url: URL подключения.

        Returns:
            Брокер FastStream.
        """
        return rabbitmq.create_broker(
            url, graceful_timeout=self._settings.PAYMENT_EVENTS_PUBLISH_TIMEOUT + 1
        )

    async def aopen(self) -> None:
        """Подключается к брокеру и объявляет обменник при запуске процесса.

        Raises:
            RuntimeError: Брокер недоступен — ошибка запуска, причина в ``__cause__``.
        """
        try:
            await self._broker.start()
            await self._broker.declare_exchange(self._exchange)
        except Exception as exc:
            raise RuntimeError(f"RabbitMQ недоступен: {exc}") from exc
        self._opened = True

    async def publish(self, events: Sequence[dtos.AnyPaymentEvent]) -> None:
        """Публикует события по одному и ждёт подтверждения размещения каждого.

        Args:
            events: События в порядке публикации.

        Raises:
            BrokerUnavailable: Соединение, канал, дедлайн или возврат сообщения — временно.
            EventPublishingFailed: Брокер отверг сообщение (``nack``) или отказал постоянно.
            RuntimeError: Публикатор не открыт (``aopen`` не вызывался).
        """
        if not self._opened:
            raise RuntimeError("Публикатор не открыт: aopen не вызывался")
        for event in events:
            message = PaymentEventMessage.from_application(event)
            routing_key = self._routing_key(event)
            try:
                confirmation = await self._broker.publish(
                    message.body(),
                    exchange=self._exchange,
                    routing_key=routing_key,
                    mandatory=True,
                    persist=True,
                    message_id=str(event.event_id),
                    content_type=CONTENT_TYPE,
                    headers=dict(message.headers()),
                    timeout=self._settings.PAYMENT_EVENTS_PUBLISH_TIMEOUT,
                )
            except Exception as exc:
                if rabbitmq.is_temporary(exc):
                    raise errors.BrokerUnavailable() from exc
                raise EventPublishingFailed(f"Не публикуется {_culprit(event)}: {exc}") from exc
            _check_confirmation(confirmation, event)
            logger.info("Событие %s опубликовано", _culprit(event))

    def _routing_key(self, event: dtos.AnyPaymentEvent) -> str:
        """Ключ маршрутизации по типу события; полноту разбора проверяет mypy.

        Args:
            event: Событие.

        Returns:
            Ключ очереди консьюмера этого типа событий.
        """
        match event:
            case dtos.PaymentCreated():
                return self._settings.PAYMENT_EVENTS_CREATED_ROUTING_KEY
            case dtos.PaymentProcessed():
                return self._settings.PAYMENT_EVENTS_PROCESSED_ROUTING_KEY
            case _:
                assert_never(event)

    async def aclose(self) -> None:
        """Закрывает соединение с брокером при остановке процесса."""
        if self._opened:
            self._opened = False
            await self._broker.stop()


def _check_confirmation(confirmation: object, event: dtos.AnyPaymentEvent) -> None:
    """Проверяет ответ брокера на публикацию.

    Возвращённое сообщение (очередь не готова) — временный сбой; отвергнутое брокером —
    постоянный: повтор даст то же.

    Args:
        confirmation: Результат ``broker.publish``.
        event: Публикуемое событие, для текста ошибки.

    Raises:
        BrokerUnavailable: Сообщение возвращено брокером.
        EventPublishingFailed: Брокер отверг сообщение.
    """
    match rabbitmq.confirmation_of(confirmation):
        case rabbitmq.Confirmation.ACKED:
            return
        case rabbitmq.Confirmation.RETURNED:
            raise errors.BrokerUnavailable() from RuntimeError(
                f"Сообщение возвращено брокером: нет очереди для {_culprit(event)}"
            )
        case rabbitmq.Confirmation.REJECTED:
            raise EventPublishingFailed(
                f"Брокер отверг {_culprit(event)}: {type(confirmation).__name__}"
            )


def _culprit(event: dtos.AnyPaymentEvent) -> str:
    """Называет событие, его тип и платёж в тексте ошибки.

    Args:
        event: Событие.

    Returns:
        Строка вида «событие … (тип) платежа …».
    """
    return f"событие {event.event_id} ({event.type}) платежа {event.payment_id}"
