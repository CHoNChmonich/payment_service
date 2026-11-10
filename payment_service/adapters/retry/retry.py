"""Повтор вызова с экспоненциальной паузой — основа декораторов повторов над портами."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

type Sleep = Callable[[float], Awaitable[None]]


class Retry:
    """Политика повторов: число попыток и пауза ``backoff · 2^(k−1)`` с потолком.

    Attributes:
        attempts: Сколько всего попыток, включая первую.
        backoff: Пауза перед второй попыткой в секундах; дальше удваивается.
        backoff_max: Потолок паузы в секундах.
    """

    def __init__(self, *, attempts: int, backoff: float, backoff_max: float = float("inf")) -> None:
        """Проверяет и запоминает параметры политики.

        Args:
            attempts: Число попыток, не меньше одной.
            backoff: Начальная пауза, неотрицательная.
            backoff_max: Потолок паузы, не меньше начальной.

        Raises:
            ValueError: Параметры вне допустимых границ.
        """
        if attempts < 1:
            raise ValueError("Число попыток не меньше одной")
        if backoff < 0:
            raise ValueError("Пауза не может быть отрицательной")
        if backoff_max < backoff:
            raise ValueError("Потолок паузы не меньше начальной паузы")
        self.attempts = attempts
        self.backoff = backoff
        self.backoff_max = backoff_max

    @property
    def total_backoff(self) -> float:
        """Сумма всех пауз между попытками в секундах, для оценки худшего случая."""
        return sum(self.pause_before(attempt) for attempt in range(2, self.attempts + 1))

    def pause_before(self, attempt: int) -> float:
        """Пауза перед попыткой с номером ``attempt`` (начиная со второй).

        Args:
            attempt: Номер попытки, от 2 до ``attempts``.

        Returns:
            Пауза в секундах.
        """
        return float(min(self.backoff * 2 ** (attempt - 2), self.backoff_max))

    async def call[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        retry_on: type[BaseException] | tuple[type[BaseException], ...],
        sleep: Sleep = asyncio.sleep,
        what: str = "операция",
    ) -> T:
        """Вызывает ``fn`` до успеха или исчерпания попыток.

        Повторяются только исключения ``retry_on``; остальные уходят наружу сразу. После
        последней неудачной попытки наружу уходит её исключение. Каждый повтор пишется в лог
        предупреждением с номером попытки: по логу видно, сколько раз отказала зависимость.

        Args:
            fn: Вызываемая корутина без аргументов.
            retry_on: Исключения, после которых вызов повторяется.
            sleep: Пауза; в тестах подменяется.
            what: Название операции для лога.

        Returns:
            Результат первого успешного вызова.
        """
        for attempt in range(1, self.attempts):
            try:
                return await fn()
            except retry_on as exc:
                pause = self.pause_before(attempt + 1)
                logger.warning(
                    "%s: попытка %d из %d не удалась (%s), повтор через %.1f с",
                    what,
                    attempt,
                    self.attempts,
                    type(exc).__name__,
                    pause,
                )
                await sleep(pause)
        return await fn()
