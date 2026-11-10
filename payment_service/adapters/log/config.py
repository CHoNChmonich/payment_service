"""Конфигурация ``logging``: один хендлер в stdout, текстовый формат, уровни сторонних логгеров."""

import logging.config
from collections.abc import Mapping
from typing import Any

from .settings import Settings

_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_TEXT_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"


def _make_config(settings: Settings, levels: Mapping[str, str]) -> dict[str, Any]:
    """Собирает dictConfig: хендлер в stdout и уровни сторонних логгеров.

    Логгеры из ``levels`` получают уровень, лишаются собственных хендлеров и уходят в корень
    (``propagate``): так сторонние библиотеки пишут нашим форматом; ``NOTSET`` означает
    «наследовать уровень корня». Ранее созданные логгеры не отключаются.

    Args:
        settings: Уровень корневого логгера.
        levels: Уровни сторонних логгеров по имени.

    Returns:
        Словарь для ``logging.config.dictConfig``.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"default": {"format": _TEXT_FORMAT, "datefmt": _DATE_FORMAT}},
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            }
        },
        "root": {"level": settings.LOG_LEVEL, "handlers": ["stdout"]},
        "loggers": {
            name: {"level": level, "handlers": [], "propagate": True}
            for name, level in levels.items()
        },
    }


def configure(settings: Settings, *, levels: Mapping[str, str]) -> None:
    """Применяет конфигурацию к ``logging`` процесса.

    Args:
        settings: Уровень корневого логгера.
        levels: Уровни сторонних логгеров по имени (см. ``_make_config``).
    """
    logging.config.dictConfig(_make_config(settings, levels))
