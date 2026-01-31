"""Data models for ActivityStream."""

from dataclasses import asdict, dataclass, field
from time import time
from typing import Any, Literal
from .constants import Defaults

@dataclass
class MarketInfo:
    """Cached market info resolved from token ID."""
    token_id: str
    market_slug: str
    event_slug: str
    outcome: str
    title: str
    condition_id: str | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None
    outcomes: list[str] | None = None
    cached_at: float = field(default_factory=time)
    
    def is_expired(self, ttl: int = Defaults.TOKEN_CACHE_TTL) -> bool:
        return (time() - self.cached_at) > ttl
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class OrderFilledEvent:
    """Raw decoded OrderFilled event from CTF Exchange."""
    order_hash: bytes
    maker: str
    taker: str
    maker_asset_id: str
    taker_asset_id: str
    maker_amount_filled: int
    taker_amount_filled: int
    fee: int
    tx_hash: str
    block_number: int
    log_index: int

@dataclass
class Trade:
    """Decoded trade from OrderFilled event."""
    tx_hash: str
    block_number: int
    log_index: int
    timestamp: int
    wallet: str
    token_id: str
    side: Literal["BUY", "SELL"]
    price: float
    size_usdc: float
    size_shares: float
    is_maker: bool = False
    market_slug: str | None = None
    market_title: str | None = None
    outcome: str | None = None
    event_slug: str | None = None
    condition_id: str | None = None
    event_count: int = 1
    source: str = "websocket"
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    def __str__(self) -> str:
        outcome = self.outcome or "Unknown"
        market = self.market_title or "Unknown Market"
        if len(market) > 50:
            market = market[:47] + "..."
        return (
            f"{self.side} ${self.size_usdc:.2f} | {outcome} @ {self.price * 100:.0f}¢ | {market}"
        )
    
    def __repr__(self) -> str:
        return (
            f"Trade(wallet={self.wallet[:10]}..., side={self.side}, "
            f"size_usdc={self.size_usdc:.2f}, price={self.price:.4f}, "
            f"outcome={self.outcome!r})"
        )

@dataclass  
class WebSocketStats:
    connected: bool
    endpoint: str | None
    subscription_id: str | None
    reconnect_count: int
    last_message_age: float | None
    events_this_connection: int
    consecutive_timeouts: int
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class ProcessorStats:
    events_received: int
    events_processed: int
    events_skipped: int
    cache_size: int
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ResolverStats:
    memory_cache_size: int
    memory_hits: int
    redis_hits: int
    api_hits: int
    api_errors: int
    
    @property
    def total_lookups(self) -> int:
        return self.memory_hits + self.redis_hits + self.api_hits
    
    @property
    def cache_hit_rate(self) -> str:
        total = self.total_lookups
        if total == 0:
            return "N/A"
        hits = self.memory_hits + self.redis_hits
        return f"{(hits / total * 100):.1f}%"
    
    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["total_lookups"] = self.total_lookups
        d["cache_hit_rate"] = self.cache_hit_rate
        return d

@dataclass
class ServiceStats:
    status: str
    uptime_seconds: float
    websocket: WebSocketStats
    processor: ProcessorStats
    resolver: ResolverStats
    tracked_wallets: int
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "uptime_seconds": self.uptime_seconds,
            "websocket": self.websocket.to_dict(),
            "processor": self.processor.to_dict(),
            "resolver": self.resolver.to_dict(),
            "tracked_wallets": self.tracked_wallets,
        }
