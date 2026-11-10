"""Composition root консьюмера новых платежей: ``python -m payment_service.launchers.consumer``.

Тот же порядок классов-неймспейсов, что у ``launchers.api``
(``Settings → Logger → Adapters → Application → Consumers``), со своим графом: БД с
``lock_timeout``, брокер, шлюз и webhook под декораторами повторов, сервис платежа, DLQ,
консьюмер. Декораторы исходящих портов собираются в ``Adapters`` рядом с адаптерами, которые
они оборачивают; сервис получает уже обёрнутые порты.
Дублирование с ``launchers.api`` сознательное: у каждого процесса свой граф и свои параметры.

Связки дедлайнов проверяет ``check_timings`` при импорте: несогласованные значения — ошибка
запуска, а не редкий зависший платёж.
"""

from functools import partial

from payment_service.adapters import (
    database,
    log,
    new_payments,
    payment_gateway,
    rabbitmq,
    webhook,
)
from payment_service.application import services
from payment_service.launchers import process


def check_timings(
    db: database.Settings,
    payments: new_payments.Settings,
    gateway: payment_gateway.Settings,
    hooks: webhook.Settings,
) -> None:
    """Проверяет согласованность дедлайнов консьюмера.

    Транзакция ``process`` держит строку платежа, пока идут обращения к шлюзу с повторами,
    ``notify`` — пока доставляется webhook с повторами. Второй экземпляр консьюмера с тем же
    сообщением ждёт блокировку, поэтому ``lock_timeout`` не меньше самого долгого удержания;
    дедлайн попытки — не меньше двух ожиданий блокировки и обоих удержаний.

    Args:
        db: Настройки БД с ``POSTGRES_LOCK_TIMEOUT_MS``.
        payments: Настройки консьюмера с дедлайном попытки.
        gateway: Настройки шлюза (худшее время обращений).
        hooks: Настройки webhook (худшее время доставки).

    Raises:
        ValueError: ``POSTGRES_LOCK_TIMEOUT_MS`` не задан или связки нарушены.
    """
    if db.POSTGRES_LOCK_TIMEOUT_MS is None:
        raise ValueError("POSTGRES_LOCK_TIMEOUT_MS обязателен для консьюмера")
    lock_timeout = db.POSTGRES_LOCK_TIMEOUT_MS / 1000
    longest_hold = max(gateway.worst_case, hooks.worst_case)
    if lock_timeout < longest_hold:
        raise ValueError(
            f"POSTGRES_LOCK_TIMEOUT_MS = {lock_timeout:.0f} с меньше худшего удержания строки "
            f"{longest_hold:.0f} с (шлюз {gateway.worst_case:.0f} с, "
            f"webhook {hooks.worst_case:.0f} с)"
        )
    needed = 2 * lock_timeout + gateway.worst_case + hooks.worst_case
    if payments.NEW_PAYMENTS_HANDLER_TIMEOUT < needed:
        raise ValueError(
            f"NEW_PAYMENTS_HANDLER_TIMEOUT = {payments.NEW_PAYMENTS_HANDLER_TIMEOUT:.0f} с меньше "
            f"2·lock_timeout + шлюз + webhook = {needed:.0f} с"
        )


class Settings:
    """Настройки процесса: БД, консьюмер, шлюз, webhook, логирование, процесс; связки дедлайнов."""

    db = database.Settings()
    payments = new_payments.Settings()
    gateway = payment_gateway.Settings()
    webhook = webhook.Settings()
    logging = log.Settings()
    process = process.Settings()
    check_timings(db, payments, gateway, webhook)


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
    """Исходящие адаптеры: БД, фабрика Unit of Work, брокер, шлюз и webhook под повторами, DLQ.

    ``gateway_client`` и ``notifier_client`` — сами адаптеры (клиент webhook закрывается хуком
    остановки), ``gateway`` и ``notifier`` — декораторы повторов над ними: сервис получает порты
    под повторами, а не адаптеры.
    """

    db = database.Database(settings=Settings.db)
    uow_factory = partial(database.UnitOfWork, db.session_factory)
    broker = rabbitmq.create_broker(
        Settings.payments.RABBITMQ_URL.get_secret_value(),
        graceful_timeout=Settings.payments.graceful_timeout,
    )
    gateway_client = payment_gateway.PaymentGateway(Settings.gateway)
    gateway = payment_gateway.RetryingPaymentGateway(gateway_client, Settings.gateway.retry_policy)
    notifier_client = webhook.WebhookNotifier(Settings.webhook)
    notifier = webhook.RetryingWebhookNotifier(notifier_client, Settings.webhook.retry_policy)
    dead_letters = new_payments.DeadLetters(Settings.payments, broker)


class Application:
    """Сервис сценариев платежа — тот же, что у HTTP-процесса."""

    payment = services.Payment(
        uow_factory=Adapters.uow_factory, gateway=Adapters.gateway, notifier=Adapters.notifier
    )


class Consumers:
    """Входящий адаптер процесса: консьюмер новых платежей с меткой «жив»."""

    new_payments = new_payments.NewPaymentsConsumer(
        handler=new_payments.NewPaymentsHandler(payment=Application.payment),
        dead_letters=Adapters.dead_letters,
        settings=Settings.payments,
        broker=Adapters.broker,
        heartbeat=process.Heartbeat(Settings.process.HEARTBEAT_FILE),
    )


def main() -> None:
    """Запускает консьюмер до SIGTERM; соединения открываются и закрываются хуками.

    Брокер подключает и останавливает сам консьюмер в ``run``.
    """
    process.run(
        Consumers.new_payments.run,
        setup=[Adapters.db.aopen],
        cleanup=[Adapters.notifier_client.aclose, Adapters.db.aclose],
    )


if __name__ == "__main__":
    main()
