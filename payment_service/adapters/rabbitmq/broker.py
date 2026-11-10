"""Фабрика брокера FastStream с едиными для проекта настройками канала и уровни логгеров."""

import logging

from faststream.rabbit import Channel, RabbitBroker

LOGGER_NAME = "payment_service.rabbitmq"


def create_broker(url: str, *, graceful_timeout: float, prefetch_count: int = 1) -> RabbitBroker:
    """Создаёт брокер; соединение открывается ``broker.start()``, а не здесь.

    Канал по умолчанию — с подтверждениями публикации (``publisher_confirms``) и исключением на
    возврат сообщения (``on_return_raises``): публикация с ``mandatory=True`` в обменник без
    подходящей очереди не считается успешной. ``prefetch_count`` ограничивает число
    неподтверждённых сообщений у подписчиков. ``fail_fast`` — ошибка первого подключения
    роняет запуск: неверный адрес брокера виден сразу. Журнал брокера — в логгер ``LOGGER_NAME``
    общим форматом процесса, а не собственным форматтером FastStream.

    Args:
        url: URL подключения ``amqp://user:password@host:port/vhost``.
        graceful_timeout: Сколько секунд ``stop()`` ждёт завершения обработки текущих
            сообщений перед закрытием канала.
        prefetch_count: Предел неподтверждённых сообщений на подписчика.

    Returns:
        Брокер FastStream.
    """
    return RabbitBroker(
        url,
        graceful_timeout=graceful_timeout,
        fail_fast=True,
        default_channel=Channel(
            prefetch_count=prefetch_count, publisher_confirms=True, on_return_raises=True
        ),
        logger=logging.getLogger(LOGGER_NAME),
    )


def logger_levels() -> dict[str, str]:
    """Уровни логгеров библиотек RabbitMQ для ``log.configure``: предупреждения и ошибки.

    aio-pika и aiormq пишут на INFO о каждом подключении и канале, FastStream — о каждом
    сообщении; остаются предупреждения (переподключение) и ошибки.

    Returns:
        Имя логгера → уровень.
    """
    return {"aio_pika": "WARNING", "aiormq": "WARNING", LOGGER_NAME: "WARNING"}
