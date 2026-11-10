"""Адаптер PostgreSQL / SQLAlchemy: таблицы, императивный маппинг, репозитории, Unit of Work.

Импорт этого пакета регистрирует маппинг сущностей на таблицы (побочный эффект модуля
``mapping``), поэтому composition root и тесты импортируют именно пакет, а не отдельные модули.
"""

from . import repositories
from .database import Database, logger_levels
from .mapping import mapper
from .settings import Settings
from .tables import metadata
from .unit_of_work import UnitOfWork

__all__ = [
    "Database",
    "Settings",
    "UnitOfWork",
    "logger_levels",
    "mapper",
    "metadata",
    "repositories",
]
