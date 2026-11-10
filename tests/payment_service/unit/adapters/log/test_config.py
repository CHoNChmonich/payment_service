import logging
from io import StringIO
from unittest.mock import patch

import pytest

from payment_service.adapters import log


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettings:
    @pytest.mark.parametrize("raw", ["debug", "Debug", "DEBUG"])
    def test__level_case_insensitive(self, raw: str) -> None:
        # Call and Assert
        assert log.Settings.model_validate({"LOG_LEVEL": raw}).LOG_LEVEL == "DEBUG"

    def test__unknown_level_rejected(self) -> None:
        # Call and Assert
        with pytest.raises(ValueError, match="LOG_LEVEL"):
            log.Settings.model_validate({"LOG_LEVEL": "LOUD"})


class TestConfigure:
    def test__writes_text_line_to_stdout(self) -> None:
        # Setup
        stream = StringIO()

        # Call
        with patch("sys.stdout", stream):
            log.configure(log.Settings(LOG_LEVEL="INFO"), levels={"noisy": "WARNING"})
            logging.getLogger("payment_service.test").info("привет")
            logging.getLogger("noisy").info("скрыто")
            logging.getLogger("noisy").warning("видно")

        # Assert
        lines = stream.getvalue().splitlines()
        assert len(lines) == 2
        assert lines[0].endswith("INFO     [payment_service.test] привет")
        assert lines[1].endswith("WARNING  [noisy] видно")
