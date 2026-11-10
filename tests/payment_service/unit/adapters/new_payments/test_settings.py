import pytest

from .support import settings


# ---------------------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------------------
class TestSettings:
    def test__budget_below_first_pause_is_rejected(self) -> None:
        # Call and Assert: иначе первый же временный сбой остановил бы процесс
        with pytest.raises(ValueError, match="RETRY_BUDGET"):
            settings(NEW_PAYMENTS_RETRY_BACKOFF=0.5, NEW_PAYMENTS_RETRY_BUDGET=0.1)
