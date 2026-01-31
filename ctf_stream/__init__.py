"""ActivityStream - Stream prediction market trades via WebSocket."""

from .client import ActivityClient
from .config import Config
from .models import (
    MarketInfo,
    OrderFilledEvent,
    ProcessorStats,
    ResolverStats,
    ServiceStats,
    Trade,
    WebSocketStats,
)

__version__ = "0.1.0"
__author__ = "hungraw"

__all__ = [
    "ActivityClient",
    "Config",
    "Trade",
    "MarketInfo",
    "OrderFilledEvent",
    "WebSocketStats",
    "ProcessorStats",
    "ResolverStats",
    "ServiceStats",
]
