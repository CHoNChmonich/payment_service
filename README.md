# Payment Service

Микросервис асинхронной обработки платежей: API принимает платёж и отвечает `202` сразу,
обработка идёт в фоне через RabbitMQ (transactional outbox → очередь `payments.new` → эмуляция
платёжного шлюза → webhook клиенту), сообщения, которые нельзя обработать, уходят в DLQ.

Стек: Python 3.12, FastAPI + Pydantic v2, SQLAlchemy 2.0 async (asyncpg), PostgreSQL 17,
Alembic, RabbitMQ 4 (FastStream + aio-pika), httpx, uv, Docker Compose.
Архитектура — гексагональная (Ports & Adapters): ядро не знает о FastAPI, SQLAlchemy и RabbitMQ.

## Быстрый старт

```bash
cp .env.example .env            # порты, учётные данные, API-ключ (AUTH_API_KEY)
docker compose up -d --build --wait
```

Поднимаются `db`, `rabbitmq` (management UI — http://localhost:15672, guest/guest), `api`
(применяет миграции и слушает http://localhost:8000; Swagger — `/docs`), `relay` (outbox →
RabbitMQ), `consumer` (обработка платежей), `webhook_sink` (эхо-приёмник webhook: пишет каждый
запрос в свой лог).

```bash
# создать платёж (все эндпоинты требуют X-API-Key из .env)
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key-change-me-0123456789" \
  -H "Idempotency-Key: order-42-attempt-1" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100.50", "currency": "RUB", "description": "Заказ №42",
       "metadata": {"order_id": 42}, "webhook_url": "http://webhook_sink:8080/payments"}'
# → 202 {"payment_id": "…", "status": "pending", "created_at": "…"}

# статус (через 2–5 с станет succeeded или failed)
curl -s http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: dev-api-key-change-me-0123456789"

# webhook, который получил приёмник
docker compose logs webhook_sink | grep <payment_id>
```

`webhook_url` указывает адрес **внутри сети compose** (`http://webhook_sink:8080/...`): `localhost`
внутри контейнера `consumer` — это сам consumer. Повтор `POST` с тем же `Idempotency-Key` и тем
же телом возвращает исходный платёж (`202`); с тем же ключом и другим телом — `409`.

Остановить стенд: `docker compose down` (с данными — `docker compose down -v`).

## Как это работает

```mermaid
sequenceDiagram
    participant C as Клиент
    participant API as api
    participant DB as PostgreSQL
    participant R as relay
    participant MQ as RabbitMQ
    participant W as consumer
    participant G as Шлюз (эмуляция)
    participant H as webhook_url

    C->>API: POST /payments (Idempotency-Key)
    API->>DB: INSERT payments + INSERT outbox (одна транзакция)
    API-->>C: 202 pending
    R->>DB: SELECT outbox FOR UPDATE SKIP LOCKED
    R->>MQ: publish payments.new (confirm, mandatory, persistent)
    R->>DB: DELETE outbox, COMMIT
    MQ->>W: payment_created
    W->>DB: SELECT payment FOR UPDATE
    W->>G: charge (2–5 с; до 3 попыток при отказе)
    W->>DB: status = succeeded | failed, COMMIT
    W->>H: POST webhook (до 3 попыток)
    W->>DB: notified_at, COMMIT
    W->>MQ: ack
```

1. **Создание.** `POST /api/v1/payments` в одной транзакции пишет платёж и событие
   `payment_created` в таблицу `outbox`. Ключ идемпотентности — уникальный индекс: гонка двух
   одинаковых запросов даёт одну строку и одно событие (`INSERT … ON CONFLICT DO NOTHING`).
2. **Outbox → RabbitMQ.** Процесс `relay` забирает события пачками (`FOR UPDATE SKIP LOCKED`),
   публикует в обменник `payments` с ключом `payments.new` и подтверждением брокера
   (publisher confirms, `mandatory`, `persistent`), затем удаляет их. Сбой публикации — событие
   остаётся и уйдёт на следующей итерации: доставка **не менее одного раза**, `message_id` =
   `event_id` события.
3. **Обработка.** `consumer` читает `payments.new` (prefetch 1, ручной `ack`). Платёж блокируется
   `FOR UPDATE`, вызывается шлюз — эмуляция: задержка 2–5 с, отказ с вероятностью 10 %. Отказ
   повторяется трижды с экспоненциальной паузой (1 с, 2 с) **в том же потоке**: это настоящие
   ошибки внешней стороны, а не повод гонять сообщение по очередям. После третьего отказа
   платёж становится `failed`, после успеха — `succeeded`; фиксируется `processed_at`.
4. **Webhook.** На `webhook_url` уходит `POST` с результатом (тело — ниже), до трёх попыток
   при недоступности получателя (сбой сети, таймаут, 5xx, 408, 429, 3xx). Ответ 2xx — доставлено,
   фиксируется `notified_at`. Ответ 4xx (кроме 408/429) — получатель отверг, повтор бессмыслен.
5. **Подтверждение.** Сообщение подтверждается после доставки webhook — или после записи в DLQ.

Повторная доставка сообщения (например, consumer упал до `ack`) безопасна: уже обработанный
платёж через шлюз не прогоняется, уже доставленный webhook не отправляется заново — оба шага
смотрят на состояние строки под блокировкой.

### Политика ошибок consumer

| Исход попытки | Действие | Примеры |
|---|---|---|
| Сообщение не по контракту | **DLQ**, `ack` | не JSON, не тот `type`, нет `payment_id` |
| Ошибка данных или правил | **DLQ**, `ack` | платежа нет в БД, получатель webhook ответил 4xx |
| Внешняя сторона недоступна после повторов | **DLQ**, `ack` | получатель webhook не отвечает после 3 попыток |
| Собственная инфраструктура недоступна | **повтор на месте** с паузой 0.5 → 10 с в пределах бюджета 120 с; по исчерпании — остановка без `ack` | БД недоступна, дедлок, `lock_timeout`, дедлайн попытки |
| Дефект кода или конфигурации | **остановка** процесса без `ack` | неожиданное исключение, нет прав в БД |

Платёж, ставший `failed` после трёх отказов шлюза и с доставленным webhook, — **обработанное**
сообщение: оно подтверждается, в DLQ не попадает. В DLQ уходит то, что сервис не смог обработать
по вине данных или внешней стороны. Сообщение, из-за которого процесс остановился, вернётся в
очередь и будет обработано первым после исправления: crash-loop на дефекте — осознанный выбор,
он виден в `docker compose ps` сразу, а не через неделю по растущей DLQ.

При лежащей БД очередь **не сливается в DLQ**: такие сбои повторяются на месте.

### Топология RabbitMQ

| Объект | Имя | Назначение |
|---|---|---|
| exchange (direct, durable) | `payments` | события платежей; relay публикует с ключом `payments.new` |
| queue (durable) | `payments.new` | новые платежи; `x-dead-letter-exchange: payments.dlx` |
| exchange (direct, durable) | `payments.dlx` | недоставленные сообщения |
| queue (durable) | `payments.new.dlq` | DLQ; сообщения кладёт consumer явно с заголовками причины |

Очередь `payments.new` (с аргументами `x-dead-letter-*`), DLX и DLQ объявляет одно место —
`new_payments.declare` при старте consumer; подписчик получает описания без объявления.
Обменник `payments` объявляют оба процесса одинаково (direct, durable, идемпотентно): relay не
зависит от того, стартовал ли consumer. В DLQ уходят **исходные**
байты тела, `message_id`, `content_type` и заголовки сообщения плюс причина:
`x-error-code` (`payment_not_found`, `webhook_unavailable`, `webhook_rejected`, `InvalidMessage`,
…), `x-error` (текст), `x-attempts`, `x-original-exchange`, `x-original-routing-key`,
`x-failed-at`. После исправления сообщение можно опубликовать обратно в `payments` как есть
(например, shovel в management UI).

Если очередь `payments.new` уже существует с другими аргументами, RabbitMQ ответит
`PRECONDITION_FAILED` — consumer остановится с этой ошибкой; удалите очередь или приведите
`NEW_PAYMENTS_*` в соответствие.

### Тело webhook

```json
{
  "payment_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "status": "succeeded",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "created_at": "2026-11-10T12:00:00Z",
  "processed_at": "2026-11-10T12:00:04Z"
}
```

Заголовки: `Content-Type: application/json`, `X-Payment-Id: <payment_id>`. Доставка — **не менее
одного раза**: получатель должен дедуплицировать по `X-Payment-Id`. Редиректы не выполняются,
cookies не сохраняются, прокси из окружения не используются.

Известные ограничения (вне задания): `webhook_url` не фильтруется по адресатам — клиент с
API-ключом может направить уведомление на внутренний адрес сети (SSRF); в production нужен
allow-list или запрет приватных диапазонов. Эмуляция шлюза идемпотентна по построению; настоящий
шлюз потребует ключ идемпотентности (`payment_id`), иначе повтор сообщения после сбоя `COMMIT`
провёл бы оплату дважды.

## API

Все эндпоинты требуют заголовок `X-API-Key` (значение — `AUTH_API_KEY` из `.env`); без него или
с неверным ключом — `401` с `WWW-Authenticate: ApiKey header="X-API-Key"`. OpenAPI — `/docs`,
`/openapi.json`.

| Метод | Путь | Ответ |
|---|---|---|
| `POST` | `/api/v1/payments` | `202` `{payment_id, status, created_at}`; `400` нет/пустой `Idempotency-Key` или невалидное тело; `409` ключ уже использован с другим телом |
| `GET` | `/api/v1/payments/{payment_id}` | `200` платёж; `404` |
| `GET` | `/api/v1/healthcheck` | `200` (без ключа; для healthcheck compose) |

Тело `POST`: `amount` (строка или число > 0, не более двух знаков), `currency` (`RUB`/`USD`/`EUR`),
`description` (до 1000 символов, по умолчанию пусто), `metadata` (JSON-объект до 4 КБ),
`webhook_url` (`http`/`https`, без учётных данных). Заголовок `Idempotency-Key` — непустой, до 255
символов.

Ответ `GET`:

```json
{
  "payment_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "idempotency_key": "order-42-attempt-1",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "webhook_url": "http://webhook_sink:8080/payments",
  "status": "succeeded",
  "created_at": "2026-11-10T12:00:00Z",
  "processed_at": "2026-11-10T12:00:04Z",
  "notified_at": "2026-11-10T12:00:04.5Z"
}
```

Сверх ТЗ: поле `notified_at` (момент доставки webhook; `null`, пока не доставлен) — по нему видно,
дошло ли уведомление, без чтения логов.

Ошибки — единая форма `{"error": "<текст>", "code": "<snake_case>", "context": {...}}`:
`validation_error` (400, `context.details` — перечень нарушений), `credentials_missing` /
`credentials_invalid` (401), `payment_not_found` (404), `idempotency_key_conflict` (409),
`storage_unavailable` (503 + `Retry-After: 1`), `internal_error` (500).

## Настройки

Все параметры — переменные окружения или `.env` (см. `.env.example`, там же комментарии):
`POSTGRES_*`, `RABBITMQ_*`, `AUTH_API_KEY`, `LOG_LEVEL`, эмуляция шлюза `PAYMENT_GATEWAY_*`
(`PAYMENT_GATEWAY_FAILURE_RATE=1` — все платежи `failed`, `0` — все `succeeded`), webhook
`WEBHOOK_*`, relay `PAYMENT_EVENTS_*`, `OUTBOX_POLLER_IDLE_INTERVAL`, `RELAY_BATCH_SIZE`,
consumer `NEW_PAYMENTS_*`.

Дедлайны связаны: `lock_timeout` консьюмера (`POSTGRES_LOCK_TIMEOUT_MS`, в compose 20 с) не
меньше худшего времени шлюза и webhook (3 × 5 с + паузы = 18 с), дедлайн попытки
`NEW_PAYMENTS_HANDLER_TIMEOUT` (90 с) не меньше `2 × lock_timeout + шлюз + webhook`. Связки
проверяются при старте consumer; `stop_grace_period` контейнера (110 с) даёт дорабатывающему
платежу завершиться и подтвердиться при `docker compose stop`.

## Разработка и тесты

```bash
uv sync                              # зависимости + проект (editable)
uv run ruff check . && uv run ruff format --check . && uv run mypy . && uv run lint-imports
uv run pytest tests/payment_service/unit                      # без инфраструктуры
uv run pytest tests/payment_service/integration/adapters/api  # HTTP на моках портов, без БД
uv run pytest                                                 # + интеграция с PostgreSQL и RabbitMQ
```

Интеграционные тесты БД ходят в `TEST_POSTGRES_*` (контейнер `db_for_tests` профиля `tests`),
тесты RabbitMQ — в `TEST_RABBITMQ_URL` (брокер стенда, временные очереди); без переменных
пропускаются. Поднять только инфраструктуру для тестов с хоста:

```bash
docker compose --profile tests up -d db_for_tests rabbitmq
```

В контейнере (отдельная БД, unit-тесты выполняются при сборке образа):

```bash
docker compose --profile tests run --rm --build tests
```

e2e на поднятом стенде (~3 мин; пишет в БД стенда, чистит DLQ, перезапускает consumer):

```bash
bash scripts/e2e.sh            # или --no-build
```

Миграции: `uv run python -m payment_service.launchers.alembic_runner upgrade head`
(`alembic.ini` нет — конфиг собирается из настроек); новая —
`... revision --autogenerate -m "<slug>" --rev-id 0002`.

## Структура

```
payment_service/
  application/            ядро: сущности, DTO, ошибки, порты (interfaces), сценарии (services)
    entities.py           Payment: State-машина pending → succeeded | failed, mark_notified
    services/             Payment (create/get/process/notify), EventRelay (outbox → publisher)
  adapters/
    api/                  FastAPI: контроллер v1, схемы запросов, DTO ответов, обработчики ошибок
    auth/                 статический API-ключ (сравнение за постоянное время)
    database/             SQLAlchemy: таблицы, imperative mapping, репозитории, Unit of Work, Alembic
    payment_events/       исходящий: публикация payment_created в RabbitMQ (confirms, mandatory)
    outbox_poller/        входящий: цикл процесса relay
    new_payments/         входящий: консьюмер payments.new — handler, policy, DLQ, повторы
    payment_gateway/      исходящий: эмуляция шлюза + Decorator повторов
    webhook/              исходящий: httpx-доставка + Decorator повторов
    rabbitmq/, retry/, log/   библиотеки адаптеров: брокер и топология, политика повторов, логи
  launchers/              composition roots: api, relay, consumer; process (сигналы, heartbeat)
tests/payment_service/    unit/ (без инфраструктуры), integration/ (api, database, rabbitmq)
deployment/entrypoints/   скрипты запуска контейнеров
scripts/e2e.sh            e2e на стенде
```

Зависимости направлены внутрь (`launchers → adapters → application`) и проверяются
`import-linter`; адаптеры не знают друг о друге — их связывает composition root. Сервисы
получают порты (`PaymentGateway`, `WebhookNotifier`) уже обёрнутыми декораторами повторов:
ядро о повторах не знает. Транзакции — Unit of Work: `async with uow_factory() as uow: …;
await uow.commit()`; откат по выходу без `commit`; обрывы соединения, дедлоки и `lock_timeout`
переводятся в `StorageUnavailable`.

Словарь: *outbox* — таблица событий, записываемых в транзакции с платежом; *relay* — процесс
доставки outbox в брокер; *DLQ* — очередь сообщений, которые не удалось обработать; *попытка* —
один прогон `process` + `notify` над сообщением; *бюджет повторов* — сколько сообщение может
повторяться из-за сбоев инфраструктуры до остановки процесса.
