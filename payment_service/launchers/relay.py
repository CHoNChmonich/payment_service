"""Composition root процесса доставки событий outbox: ``python -m payment_service.launchers.relay``.

Тот же порядок классов-неймспейсов, что у ``launchers.api``
(``Settings → Logger → Adapters → Application → Pollers``), со своим минимальным графом: БД,
публикатор RabbitMQ, сценарий ``EventRelay``, опрос outbox. Дублирование с ``launchers.api``
сознательное: у каждого процесса свой граф и свои параметры.
"""

from functools import partial

from payment_service.adapters import database, log, outbox_poller, payment_events, rabbitmq
from payment_service.application import services
from payment_service.launchers import config, process


class Settings:
    """Настройки процесса: БД, RabbitMQ, опрос outbox, приложение, логирование, процесс."""

    db = database.Settings()
    events = payment_events.Settings()
    poller = outbox_poller.Settings()
    service = config.Settings()
    logging = log.Settings()
    process = process.Settings()


class Logger:
    """Логирование: уровень и формат из настроек; SQLAlchemy и RabbitMQ — в общий хендлер."""

    log.configure(
        Settings.logging,
        levels={**database.logger_levels(Settings.db), **rabbitmq.logger_levels()},
    )


class Adapters:
    """Исходящие адаптеры: БД, фабрика Unit of Work, публикатор событий."""

    db = database.Database(settings=Settings.db)
    publisher = payment_events.EventPublisher(Settings.events)
    uow_factory = partial(database.UnitOfWork, db.session_factory)


class Application:
    """Сценарий доставки событий; размер пачки — из настроек приложения."""

    relay = services.EventRelay(
        uow_factory=Adapters.uow_factory,
        publisher=Adapters.publisher,
        batch_size=Settings.service.RELAY_BATCH_SIZE,
    )


class Pollers:
    """Входящий адаптер процесса: опрос outbox с меткой «жив»."""

    outbox = outbox_poller.OutboxPoller(
        relay=Application.relay,
        settings=Settings.poller,
        heartbeat=process.Heartbeat(Settings.process.HEARTBEAT_FILE),
    )


def main() -> None:
    """Запускает опрос outbox до SIGTERM; соединения открываются и закрываются хуками."""
    process.run(
        Pollers.outbox.run,
        setup=[Adapters.db.aopen, Adapters.publisher.aopen],
        cleanup=[Adapters.publisher.aclose, Adapters.db.aclose],
    )


if __name__ == "__main__":
    main()
