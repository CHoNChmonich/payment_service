"""Императивный маппинг сущностей ядра на таблицы.

Ядро не знает о SQLAlchemy: dataclass ``entities.Payment`` инструментируется здесь при импорте.
"""

from sqlalchemy.orm import registry

from payment_service.application import entities

from . import tables

mapper = registry()

mapper.map_imperatively(entities.Payment, tables.payments)
