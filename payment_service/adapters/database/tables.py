"""Схема БД: таблицы и ``metadata`` (единственный источник для Alembic и для маппинга)."""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    MetaData,
    Numeric,
    String,
    Table,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

from payment_service.application.dtos import (
    MAX_DESCRIPTION_LENGTH,
    MAX_IDEMPOTENCY_KEY_LENGTH,
    MAX_WEBHOOK_URL_LENGTH,
)
from payment_service.application.entities import Currency, PaymentStatus

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=naming_convention)

# Платежи. В БД хранятся значения перечислений (pending/succeeded/failed, RUB/USD/EUR), а не
# имена членов; CHECK-ограничения охраняют эти списки на стороне БД. Уникальность
# idempotency_key — то, на чём держится идемпотентность создания. Длины строк — из границ
# DTO (dtos.base): вход, прошедший валидацию, гарантированно помещается.
payments = Table(
    "payments",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("idempotency_key", String(MAX_IDEMPOTENCY_KEY_LENGTH), nullable=False, unique=True),
    Column("amount", Numeric(18, 2), nullable=False),
    Column(
        "currency",
        Enum(
            Currency,
            name="currency",
            native_enum=False,
            create_constraint=True,
            length=3,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
    ),
    Column("description", String(MAX_DESCRIPTION_LENGTH), nullable=False),
    Column("metadata", JSONB, nullable=False),
    Column("webhook_url", String(MAX_WEBHOOK_URL_LENGTH), nullable=False),
    Column(
        "status",
        Enum(
            PaymentStatus,
            name="payment_status",
            native_enum=False,
            create_constraint=True,
            length=16,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
    ),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=True),
    Column("notified_at", DateTime(timezone=True), nullable=True),
)

# Transactional outbox: события платежей до публикации. ``id`` (bigserial) задаёт порядок
# добавления и выборки пачек. Идентификатор платежа лежит внутри payload: порядок между
# платежами не нужен, а внешнего ключа нет — событие уходит наружу, а не часть модели платежа.
outbox = Table(
    "outbox",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("event_id", Uuid, nullable=False, unique=True),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
