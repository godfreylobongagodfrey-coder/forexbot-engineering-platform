from dataclasses import dataclass
from datetime import datetime
from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

@dataclass(frozen=True)
class Candle:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0
    complete: bool = True
    spread_points: float | None = None
