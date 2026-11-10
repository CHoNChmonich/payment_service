"""Inbox входящих сообщений; события «платёж проведён» для платежей без уведомления.

До этой ревизии один консьюмер проводил платёж и уведомлял клиента по одному сообщению. Теперь
уведомление идёт по своему событию ``payment_processed``: платежам, обработанным старым кодом,
но не уведомлённым, событие добавляется в outbox здесь — иначе их никто не уведомит. Откат
симметричен: неуведомлённым возвращается ``payment_created``, по которому их уведомит прежний
консьюмер.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-10 18:00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# По событию типа :event_type в outbox на каждый обработанный неуведомлённый платёж. Формат
# документа — ``model_dump(mode="json")`` событий ``dtos.PaymentProcessed`` /
# ``dtos.PaymentCreated``: event_id в колонке и в документе — одно значение из CTE (по нему relay
# удаляет строку). Момент: у «проведён» — processed_at (нет значения — created_at: событие с
# occurred_at = null relay не прочитал бы), у «создан» — created_at.
_EVENTS_FOR_UNNOTIFIED = sa.text("""
    WITH unnotified AS (
        SELECT
            gen_random_uuid() AS event_id,
            id AS payment_id,
            CASE
                WHEN :event_type = 'payment_processed' THEN COALESCE(processed_at, created_at)
                ELSE created_at
            END AS occurred_at
        FROM payments
        WHERE status <> 'pending' AND notified_at IS NULL
    )
    INSERT INTO outbox (event_id, payload)
    SELECT
        event_id,
        jsonb_build_object(
            'event_id', event_id::text,
            'occurred_at', to_char(
                occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ),
            'payment_id', payment_id::text,
            'type', CAST(:event_type AS text)
        )
    FROM unnotified
""")


def upgrade() -> None:
    """Создаёт таблицу inbox и события ``payment_processed`` для платежей без уведомления."""
    op.create_table(
        "inbox",
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("fingerprint", sa.String(length=80), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("message_id", name=op.f("pk_inbox")),
    )
    op.execute(_EVENTS_FOR_UNNOTIFIED.bindparams(event_type="payment_processed"))


def downgrade() -> None:
    """Удаляет таблицу inbox и события ``payment_processed``; неуведомлённым — ``payment_created``.

    Прежний relay события ``payment_processed`` не читает, а прежний консьюмер уведомляет по
    ``payment_created``: проведённый платёж через шлюз не идёт, webhook уходит. Без этого
    платежи, ждавшие уведомления (в outbox или в очереди ``payments.processed``), остались бы
    без него.
    """
    op.execute("DELETE FROM outbox WHERE payload->>'type' = 'payment_processed'")
    op.execute(_EVENTS_FOR_UNNOTIFIED.bindparams(event_type="payment_created"))
    op.drop_table("inbox")
