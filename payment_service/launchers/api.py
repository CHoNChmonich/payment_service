"""Composition root HTTP-сервиса: ``uvicorn payment_service.launchers.api:app``.

Граф зависимостей собирается в классах-неймспейсах при импорте модуля
(``Settings → Logger → Adapters → Application → Controllers``) — без DI-контейнера и без
FastAPI-``Depends``. Все адаптеры и сервисы — синглтоны процесса; ошибка конфигурации
проявляется на старте, а не на первом запросе.

Сервису платежа нужны шлюз и webhook, хотя HTTP-процесс платежи не проводит и не уведомляет:
входящий порт ``Payment`` общий для API и консьюмеров (компромисс ISP). Неиспользуемые порты
создаются без соединений; под декоратором повторов — только порт, который процесс вызывает
(шлюз в ``launchers.consumer``, webhook в ``launchers.notifier``).
Имена логгеров uvicorn живут здесь, а не в HTTP-адаптере: сервер выбирает launcher; уровень
``NOTSET`` означает «наследовать ``LOG_LEVEL``».
"""

from functools import partial

from payment_service.adapters import api, auth, database, log, payment_gateway, webhook
from payment_service.application import services

UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


class Settings:
    """Настройки процесса: БД, API-ключ, шлюз, webhook, логирование."""

    db = database.Settings()
    auth = auth.Settings()
    gateway = payment_gateway.Settings()
    webhook = webhook.Settings()
    logging = log.Settings()


class Logger:
    """Логирование: уровень и формат из настроек; uvicorn, SQLAlchemy и httpx — в общий хендлер."""

    log.configure(
        Settings.logging,
        levels={
            **database.logger_levels(Settings.db),
            **dict.fromkeys(UVICORN_LOGGERS, "NOTSET"),
            **webhook.logger_levels(),
        },
    )


class Adapters:
    """Исходящие адаптеры: БД, фабрика Unit of Work, аутентификатор, шлюз и webhook под повторами.

    ``gateway_client`` и ``notifier_client`` — сами адаптеры (клиент webhook закрывается хуком
    остановки), ``gateway`` и ``notifier`` — декораторы повторов над ними.
    """

    db = database.Database(settings=Settings.db)
    uow_factory = partial(database.UnitOfWork, db.session_factory)
    authenticator = auth.Authenticator(Settings.auth)
    gateway_client = payment_gateway.PaymentGateway(Settings.gateway)
    gateway = payment_gateway.RetryingPaymentGateway(gateway_client, Settings.gateway.retry_policy)
    notifier_client = webhook.WebhookNotifier(Settings.webhook)
    notifier = webhook.RetryingWebhookNotifier(notifier_client, Settings.webhook.retry_policy)


class Application:
    """Сервис сценариев платежа."""

    payment = services.Payment(
        uow_factory=Adapters.uow_factory, gateway=Adapters.gateway, notifier=Adapters.notifier
    )


class Controllers:
    """Входящие адаптеры: контроллер API v1 с внедрёнными аутентификатором и портом сценариев."""

    v1_controller = api.V1Controller(
        authenticator=Adapters.authenticator, payment=Application.payment
    )


lifespan = api.create_lifespan(
    setup=[Adapters.db.aopen], cleanup=[Adapters.notifier_client.aclose, Adapters.db.aclose]
)
app = api.create_app(v1_controller=Controllers.v1_controller, lifespan=lifespan)
