"""Конфигурация логирования — инфраструктура, а не адаптер порта.

Логирование — сквозная функциональность: любой слой берёт ``logging.getLogger(__name__)`` и
ничего не знает о хендлерах и форматах. Их задаёт этот пакет, а применяет composition root
(``configure``); сборка словаря ``dictConfig`` — деталь модуля ``config``. Зависимостей на
другие адаптеры у пакета нет.
"""

from .config import configure
from .settings import Settings

__all__ = ["Settings", "configure"]
