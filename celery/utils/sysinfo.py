"""System information utilities."""
from __future__ import annotations

import os
from math import ceil

from kombu.utils.objects import cached_property

__all__ = ('load_average', 'df')


if hasattr(os, 'getloadavg'):

    def _load_average() -> tuple[float, ...]:
        return tuple(ceil(l * 1e2) / 1e2 for l in os.getloadavg())

else:  # pragma: no cover
    # Windows doesn't have getloadavg
    def _load_average() -> tuple[float, ...]:
        return 0.0, 0.0, 0.0,


def load_average() -> tuple[float, ...]:
    """Return system load average as a triple."""
    return _load_average()


class df:
    """Disk information."""

    def __init__(self, path: str | bytes | os.PathLike) -> None:
        self.path = path

    @property
    def total_blocks(self) -> float:
        pass

    @property
    def available(self) -> float:
        pass

    @property
    def capacity(self) -> int:
        pass

    @cached_property
    def stat(self) -> os.statvfs_result:
        pass
