"""Политика повторов — библиотека без портов для декораторов ``Retrying*`` над портами ядра."""

from .retry import Retry, Sleep

__all__ = ["Retry", "Sleep"]
