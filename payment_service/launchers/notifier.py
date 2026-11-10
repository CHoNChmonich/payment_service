"""Composition root консьюмера уведомлений: ``python -m payment_service.launchers.notifier``.

Тот же порядок классов-неймспейсов, что у ``launchers.consumer``
(``Settings → Logger → Adapters → Application → Consumers``), со своим графом: БД с
``lock_timeout``, брокер, доставка webhook под декоратором повторов, сервис платежа, DLQ,
консьюмер очереди ``payments.processed`` (сценарий ``notify``). Отдельный процесс: медленные
получатели webhook не задерживают проведение платежей, у каждого процесса свой ``lock_timeout``.

Компромисс ISP: ``services.Payment`` требует и шлюз, и доставку webhook, а этому процессу нужна
только доставка; эмуляция шлюза соединений не открывает — как неиспользуемые порты в
``launchers.api``.

Связки дедлайнов проверяет ``check_timings`` при импорте: несогласованные значения — ошибка
запуска, а не редкое зависшее уведомление.
"""

from functools import partial

from payment_service.adapters import (
    database,
    log,
    payment_gateway,
    processed_payments,
    rabbitmq,
    webhook,
)
from payment_service.application import services
from payment_service.launchers import process


def check_timings(
    db: database.Settings, processed: processed_payments.Settings, hooks: webhook.Settings
) -> None:
    """Проверяет согласованность дедлайнов консьюмера уведомлений.

    Транзакция ``notify`` держит регистрацию события в inbox и строку платежа, пока
    доставляется webhook с повторами. Копия того же события в другом экземпляре ждёт вставку в
    inbox, поэтому ``lock_timeout`` не меньше худшего времени доставки; дедлайн попытки — не
    меньше одного ожидания блокировки и худшего времени доставки.

    Args:
        db: Настройки БД с ``POSTGRES_LOCK_TIMEOUT_MS``.
        processed: Настройки консьюмера с дедлайном попытки.
        hooks: Настройки webhook (худшее время доставки).

    Raises:
        ValueError: ``POSTGRES_LOCK_TIMEOUT_MS`` не задан или связки нарушены.
    """
    if db.POSTGRES_LOCK_TIMEOUT_MS is None:
        raise ValueError("POSTGRES_LOCK_TIMEOUT_MS обязателен для консьюмера уведомлений")
    lock_timeout = db.POSTGRES_LOCK_TIMEOUT_MS / 1000
    if lock_timeout < hooks.worst_case:
        raise ValueError(
            f"POSTGRES_LOCK_TIMEOUT_MS = {lock_timeout:.0f} с меньше худшего удержания "
            f"блокировки — времени доставки webhook {hooks.worst_case:.0f} с"
        )
    needed = lock_timeout + hooks.worst_case
    timeout = processed.PROCESSED_PAYMENTS_HANDLER_TIMEOUT
    if timeout < needed:
        raise ValueError(
            f"PROCESSED_PAYMENTS_HANDLER_TIMEOUT = {timeout:.0f} с меньше "
            f"lock_timeout + webhook = {needed:.0f} с"
        )


class Settings:
    """Настройки процесса: БД, консьюмер, шлюз, webhook, логирование, процесс; связки дедлайнов."""

    db = database.Settings()
    processed = processed_payments.Settings()
    gateway = payment_gateway.Settings()
    webhook = webhook.Settings()
    logging = log.Settings()
    process = process.Settings()
    check_timings(db, processed, webhook)


class Logger:
    """Логирование: уровень и формат из настроек; SQLAlchemy, RabbitMQ и httpx — в общий хендлер."""

    log.configure(
        Settings.logging,
        levels={
            **database.logger_levels(Settings.db),
            **rabbitmq.logger_levels(),
            **webhook.logger_levels(),
        },
    )


class Adapters:
    """Исходящие адаптеры: БД, фабрика Unit of Work, брокер, webhook под повторами, DLQ.

    ``notifier_client`` — сам адаптер (клиент закрывается хуком остановки), ``notifier`` —
    декоратор повторов над ним: сервис получает порт под повторами. ``gateway`` нужен только
    конструктору сервиса.
    """

    db = database.Database(settings=Settings.db)
    uow_factory = partial(database.UnitOfWork, db.session_factory)
    consumer_config = Settings.processed.consumer_config
    broker = rabbitmq.create_broker(
        Settings.processed.RABBITMQ_URL.get_secret_value(),
        graceful_timeout=consumer_config.graceful_timeout,
    )
    gateway = payment_gateway.PaymentGateway(Settings.gateway)
    notifier_client = webhook.WebhookNotifier(Settings.webhook)
    notifier = webhook.RetryingWebhookNotifier(notifier_client, Settings.webhook.retry_policy)
    dead_letters = rabbitmq.DeadLetters(
        consumer_config.dead_letter_target, broker, processed_payments.error_code
    )


class Application:
    """Сервис сценариев платежа — тот же, что у HTTP-процесса."""

    payment = services.Payment(
        uow_factory=Adapters.uow_factory, gateway=Adapters.gateway, notifier=Adapters.notifier
    )


class Consumers:
    """Входящий адаптер процесса: консьюмер очереди проведённых платежей с меткой «жив»."""

    processed_payments = rabbitmq.QueueConsumer(
        handler=processed_payments.ProcessedPaymentsHandler(payment=Application.payment).handle,
        action_for=processed_payments.action_for,
        dead_letters=Adapters.dead_letters,
        config=Adapters.consumer_config,
        broker=Adapters.broker,
        heartbeat=process.Heartbeat(Settings.process.HEARTBEAT_FILE),
    )


def main() -> None:
    """Запускает консьюмер уведомлений до SIGTERM; соединения открываются и закрываются хуками.

    Брокер подключает и останавливает сам консьюмер в ``run``.
    """
    process.run(
        Consumers.processed_payments.run,
        setup=[Adapters.db.aopen],
        cleanup=[Adapters.notifier_client.aclose, Adapters.db.aclose],
    )


if __name__ == "__main__":
    main()
