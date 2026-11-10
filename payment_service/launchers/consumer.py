"""Composition root консьюмера новых платежей: ``python -m payment_service.launchers.consumer``.

Тот же порядок классов-неймспейсов, что у ``launchers.api``
(``Settings → Logger → Adapters → Application → Consumers``), со своим графом: БД с
``lock_timeout``, брокер, шлюз под декоратором повторов, сервис платежа, DLQ, консьюмер
очереди ``payments.new`` (сценарий ``process``). Уведомление клиента — процесс
``launchers.notifier``. Декораторы исходящих портов собираются в ``Adapters`` рядом с
адаптерами, которые они оборачивают; сервис получает уже обёрнутые порты.
Дублирование с ``launchers.api`` и ``launchers.notifier`` сознательное: у каждого процесса свой
граф и свои параметры.

Компромисс ISP: ``services.Payment`` требует и шлюз, и доставку webhook, а этому процессу нужен
только шлюз; клиент webhook создаётся без соединений (они ленивые) и закрывается хуком
остановки — как неиспользуемые порты в ``launchers.api``.

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
    db: database.Settings, payments: new_payments.Settings, gateway: payment_gateway.Settings
) -> None:
    """Проверяет согласованность дедлайнов консьюмера новых платежей.

    Транзакция ``process`` держит регистрацию события в inbox и строку платежа, пока идут
    обращения к шлюзу с повторами. Копия того же события в другом экземпляре консьюмера ждёт
    вставку в inbox, поэтому ``lock_timeout`` не меньше худшего времени шлюза; дедлайн попытки
    — не меньше одного ожидания блокировки и худшего времени шлюза (в штатном потоке строку
    платежа после регистрации события никто не держит).

    Args:
        db: Настройки БД с ``POSTGRES_LOCK_TIMEOUT_MS``.
        payments: Настройки консьюмера с дедлайном попытки.
        gateway: Настройки шлюза (худшее время обращений).

    Raises:
        ValueError: ``POSTGRES_LOCK_TIMEOUT_MS`` не задан или связки нарушены.
    """
    if db.POSTGRES_LOCK_TIMEOUT_MS is None:
        raise ValueError("POSTGRES_LOCK_TIMEOUT_MS обязателен для консьюмера")
    lock_timeout = db.POSTGRES_LOCK_TIMEOUT_MS / 1000
    if lock_timeout < gateway.worst_case:
        raise ValueError(
            f"POSTGRES_LOCK_TIMEOUT_MS = {lock_timeout:.0f} с меньше худшего удержания "
            f"блокировки — времени шлюза {gateway.worst_case:.0f} с"
        )
    needed = lock_timeout + gateway.worst_case
    if payments.NEW_PAYMENTS_HANDLER_TIMEOUT < needed:
        raise ValueError(
            f"NEW_PAYMENTS_HANDLER_TIMEOUT = {payments.NEW_PAYMENTS_HANDLER_TIMEOUT:.0f} с меньше "
            f"lock_timeout + шлюз = {needed:.0f} с"
        )


class Settings:
    """Настройки процесса: БД, консьюмер, шлюз, webhook, логирование, процесс; связки дедлайнов."""

    db = database.Settings()
    payments = new_payments.Settings()
    gateway = payment_gateway.Settings()
    webhook = webhook.Settings()
    logging = log.Settings()
    process = process.Settings()
    check_timings(db, payments, gateway)


class Logger:
    """Логирование: уровень и формат из настроек; SQLAlchemy и RabbitMQ — в общий хендлер."""

    log.configure(
        Settings.logging,
        levels={**database.logger_levels(Settings.db), **rabbitmq.logger_levels()},
    )


class Adapters:
    """Исходящие адаптеры: БД, фабрика Unit of Work, брокер, шлюз под повторами, DLQ.

    ``gateway_client`` — сам адаптер шлюза, ``gateway`` — декоратор повторов над ним: сервис
    получает порт под повторами. ``notifier_client`` нужен только конструктору сервиса.
    """

    db = database.Database(settings=Settings.db)
    uow_factory = partial(database.UnitOfWork, db.session_factory)
    consumer_config = Settings.payments.consumer_config
    broker = rabbitmq.create_broker(
        Settings.payments.RABBITMQ_URL.get_secret_value(),
        graceful_timeout=consumer_config.graceful_timeout,
    )
    gateway_client = payment_gateway.PaymentGateway(Settings.gateway)
    gateway = payment_gateway.RetryingPaymentGateway(gateway_client, Settings.gateway.retry_policy)
    notifier_client = webhook.WebhookNotifier(Settings.webhook)
    dead_letters = rabbitmq.DeadLetters(
        consumer_config.dead_letter_target, broker, new_payments.error_code
    )


class Application:
    """Сервис сценариев платежа — тот же, что у HTTP-процесса."""

    payment = services.Payment(
        uow_factory=Adapters.uow_factory,
        gateway=Adapters.gateway,
        notifier=Adapters.notifier_client,
    )


class Consumers:
    """Входящий адаптер процесса: консьюмер очереди новых платежей с меткой «жив»."""

    new_payments = rabbitmq.QueueConsumer(
        handler=new_payments.NewPaymentsHandler(payment=Application.payment).handle,
        action_for=new_payments.action_for,
        dead_letters=Adapters.dead_letters,
        config=Adapters.consumer_config,
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
