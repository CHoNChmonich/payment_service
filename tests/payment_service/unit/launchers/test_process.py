import asyncio
import logging
import signal
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import cast
from unittest.mock import AsyncMock, call

import pytest

from payment_service.launchers import process


# ---------------------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------------------
class SignalRecorder:
    """Фейки обработчиков сигналов: настоящие сигналы процессу pytest не нужны.

    ``loop_supported`` — путь Linux (``loop.add_signal_handler``), иначе путь Windows
    (``NotImplementedError`` → ``signal.signal``).
    """

    def __init__(self, loop_supported: bool) -> None:
        self.loop_supported = loop_supported
        self.loop_handlers: dict[int, Callable[[], None]] = {}
        self.removed: list[int] = []
        self.signal_handlers: dict[int, object] = {}
        # Каждый вызов signal.signal: сигнал, новый обработчик, вытесненный обработчик
        self.history: list[tuple[int, object, object]] = []
        self.installed: dict[int, object] = {}

    def fire(self, signum: int) -> None:
        """Сымитировать сигнал: вызвать тот обработчик, который поставил код."""
        if self.loop_supported:
            self.loop_handlers[signum]()
        else:
            handler = cast(Callable[[int, FrameType | None], None], self.signal_handlers[signum])
            handler(signum, None)

    async def snapshot(self) -> None:
        """Хук запуска: запомнить обработчики ``signal.signal``, поставленные кодом до хуков."""
        self.installed = dict(self.signal_handlers)

    def restored(self) -> bool:
        """Каждый обработчик из ``snapshot`` позже заменён тем, который он вытеснил.

        Сравнение с конечным состоянием не годится: SIGINT в конце ставит и ``asyncio.run``.
        """
        for signum, handler in self.installed.items():
            sets = [(new, old) for s, new, old in self.history if s == signum]
            index = next(i for i, (new, _) in enumerate(sets) if new is handler)
            displaced = sets[index][1]
            if not any(new is displaced for new, _ in sets[index + 1 :]):
                return False
        return True


@pytest.fixture(scope="function", params=[True, False], ids=["linux", "windows"])
def signals(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> SignalRecorder:
    recorder = SignalRecorder(loop_supported=request.param)

    def add_signal_handler(
        _loop: asyncio.AbstractEventLoop, signum: int, callback: Callable[[], None]
    ) -> None:
        if not recorder.loop_supported:
            raise NotImplementedError
        recorder.loop_handlers[signum] = callback

    def remove_signal_handler(_loop: asyncio.AbstractEventLoop, signum: int) -> bool:
        recorder.removed.append(signum)
        return True

    def fake_signal(signum: int, handler: object) -> object:
        previous = recorder.signal_handlers.get(signum, signal.SIG_DFL)
        recorder.signal_handlers[signum] = handler
        recorder.history.append((signum, handler, previous))
        return previous

    loop = asyncio.new_event_loop()  # класс цикла, который создаст asyncio.run
    loop.close()
    monkeypatch.setattr(type(loop), "add_signal_handler", add_signal_handler)
    monkeypatch.setattr(type(loop), "remove_signal_handler", remove_signal_handler)
    monkeypatch.setattr(signal, "signal", fake_signal)
    return recorder


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestRun:
    def test__hooks_around_main(self, signals: SignalRecorder) -> None:
        # Setup
        hooks = AsyncMock()

        async def main(stop: asyncio.Event) -> None:
            await hooks.main()

        # Call
        process.run(main, setup=[hooks.setup], cleanup=[hooks.cleanup])

        # Assert
        assert hooks.mock_calls == [call.setup(), call.main(), call.cleanup()]

    def test__main_error_exits_with_1_after_cleanup(self, signals: SignalRecorder) -> None:
        # Setup
        cleanup = AsyncMock()

        async def main(stop: asyncio.Event) -> None:
            raise RuntimeError("дефект")

        # Call and Assert
        with pytest.raises(SystemExit) as exc_info:
            process.run(main, cleanup=[cleanup])

        assert exc_info.value.code == 1
        cleanup.assert_awaited_once()

    def test__setup_error_exits_without_main(self, signals: SignalRecorder) -> None:
        # Setup
        main = AsyncMock()
        cleanup = AsyncMock()
        failing = AsyncMock(side_effect=RuntimeError("БД недоступна"))

        # Call and Assert
        with pytest.raises(SystemExit):
            process.run(main, setup=[failing], cleanup=[cleanup])

        main.assert_not_awaited()
        cleanup.assert_not_awaited()

    def test__cleanup_error_is_logged_and_others_run(
        self, signals: SignalRecorder, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Setup
        second = AsyncMock()

        async def main(stop: asyncio.Event) -> None:
            return None

        # Call
        with caplog.at_level(logging.WARNING, logger="payment_service.launchers.process"):
            process.run(main, cleanup=[AsyncMock(side_effect=RuntimeError("сбой")), second])

        # Assert
        second.assert_awaited_once()
        assert [record.levelno for record in caplog.records] == [logging.WARNING]

    @pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
    def test__signal_sets_stop(self, signum: int, signals: SignalRecorder) -> None:
        # Setup
        async def main(stop: asyncio.Event) -> None:
            signals.fire(signum)
            async with asyncio.timeout(1):
                await stop.wait()

        # Call and Assert: цикл вернулся сам — событие остановки поставлено
        process.run(main)

    def test__second_signal_interrupts(self, signals: SignalRecorder) -> None:
        # Setup: процесс завис на итерации и не смотрит на событие остановки
        async def main(stop: asyncio.Event) -> None:
            signals.fire(signal.SIGTERM)
            await asyncio.sleep(0)
            signals.fire(signal.SIGTERM)
            await asyncio.sleep(10)

        # Call and Assert
        with pytest.raises(KeyboardInterrupt):
            process.run(main)

    def test__handlers_restored(self, signals: SignalRecorder) -> None:
        # Setup
        async def main(stop: asyncio.Event) -> None:
            return None

        # Call
        process.run(main, setup=[signals.snapshot])

        # Assert: после выхода обработчиков процесса не осталось
        if signals.loop_supported:
            assert sorted(signals.removed) == sorted([signal.SIGTERM, signal.SIGINT])
        else:
            assert set(signals.installed) == {signal.SIGTERM, signal.SIGINT}
            assert signals.restored()

    @pytest.mark.parametrize("failing", ["setup", "main"])
    def test__handlers_restored_after_error(self, failing: str, signals: SignalRecorder) -> None:
        # Setup
        error = AsyncMock(side_effect=RuntimeError("сбой"))

        async def main(stop: asyncio.Event) -> None:
            if failing == "main":
                await error()

        # Call
        with pytest.raises(SystemExit):
            process.run(main, setup=[signals.snapshot, *([error] if failing == "setup" else [])])

        # Assert: обработчики процесса не остаются и после падения
        if signals.loop_supported:
            assert sorted(signals.removed) == sorted([signal.SIGTERM, signal.SIGINT])
        else:
            assert set(signals.installed) == {signal.SIGTERM, signal.SIGINT}
            assert signals.restored()


class TestHeartbeat:
    def test__touch_and_age(self, tmp_path: Path) -> None:
        # Setup
        heartbeat = process.Heartbeat(tmp_path / "alive")

        # Call
        before = heartbeat.age()
        heartbeat()

        # Assert
        assert before is None
        age = heartbeat.age()
        assert age is not None
        assert 0 <= age < 5

    def test__disabled(self) -> None:
        # Call and Assert
        heartbeat = process.Heartbeat(None)
        heartbeat()
        assert heartbeat.age() is None


class TestCheckHeartbeat:
    def test__fresh_and_missing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Setup
        path = tmp_path / "alive"
        monkeypatch.setenv("HEARTBEAT_FILE", str(path))

        # Call
        missing = process.check_heartbeat(60)
        process.Heartbeat(path)()
        fresh = process.check_heartbeat(60)
        stale = process.check_heartbeat(0)

        # Assert
        assert (missing, fresh, stale) == (1, 0, 1)
