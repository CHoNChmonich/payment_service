"""Запуск Alembic с конфигурацией из настроек — ``alembic.ini`` не нужен.

python -m payment_service.launchers.alembic_runner upgrade head
python -m payment_service.launchers.alembic_runner revision --autogenerate -m "<slug>" --rev-id 0002
python -m payment_service.launchers.alembic_runner check
"""

import sys

from alembic.config import CommandLine, Config

from payment_service.adapters import database, log

SCRIPT_LOCATION = "payment_service.adapters.database:alembic"
VERSION_LOCATIONS = "payment_service.adapters.database:migrations"
FILE_TEMPLATE = "%%(rev)s_%%(slug)s"


def make_config(settings: database.Settings) -> Config:
    """Собирает конфигурацию Alembic из настроек БД и констант модуля.

    Символы ``%`` в URL экранируются: ConfigParser трактует их как подстановку.

    Args:
        settings: Параметры подключения к PostgreSQL.

    Returns:
        Конфигурация с расположением скриптов, миграций, шаблоном имён файлов и URL БД.
    """
    alembic_config = Config()
    alembic_config.set_main_option("script_location", SCRIPT_LOCATION)
    alembic_config.set_main_option("version_locations", VERSION_LOCATIONS)
    # Пакетный путь содержит «:», поэтому не «os» (в Linux это «:»): одна строка — один путь
    alembic_config.set_main_option("path_separator", "newline")
    alembic_config.set_main_option("file_template", FILE_TEMPLATE)
    alembic_config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    return alembic_config


def run_cmd(*args: str) -> None:
    """Выполняет команду Alembic с аргументами командной строки.

    Логирование настраивается так же, как у HTTP-сервиса: логгер ``alembic`` хендлеров не
    вешает (``env.py`` не вызывает ``fileConfig``) и наследует корневой.

    Args:
        *args: Аргументы в формате CLI Alembic, например ``upgrade head``.
    """
    db_settings = database.Settings()
    log.configure(log.Settings(), levels=database.logger_levels(db_settings))
    cli = CommandLine(prog="python -m payment_service.launchers.alembic_runner")
    cli.run_cmd(make_config(db_settings), cli.parser.parse_args(args))


if __name__ == "__main__":
    run_cmd(*sys.argv[1:])
