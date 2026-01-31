"""Configuration for ActivityStream."""

from dataclasses import dataclass, field
from typing import Sequence
from .constants import (
    DEFAULT_CONTRACTS,
    Defaults,
    RPCEndpoints,
)

@dataclass
class Config:
    """ActivityStream configuration."""

    rpc_endpoints: Sequence[str] = field(
        default_factory=lambda: list(RPCEndpoints.DEFAULT_ENDPOINTS)
    )
    redis_url: str | None = None
    contracts: Sequence[str] = field(default_factory=lambda: list(DEFAULT_CONTRACTS))
    
    ws_ping_interval: int = Defaults.PING_INTERVAL
    ws_ping_timeout: int = Defaults.PING_TIMEOUT
    ws_idle_timeout: int = Defaults.IDLE_TIMEOUT
    
    reconnect_delays: Sequence[int] = field(
        default_factory=lambda: list(Defaults.RECONNECT_DELAYS)
    )
    reconnect_jitter_max: float = Defaults.RECONNECT_JITTER_MAX
    max_timeouts_before_rotate: int = Defaults.MAX_TIMEOUTS_BEFORE_ROTATE
    
    token_cache_ttl: int = Defaults.TOKEN_CACHE_TTL
    dedup_cache_size: int = Defaults.DEDUP_CACHE_SIZE
    api_timeout: float = Defaults.API_TIMEOUT
    
    def __post_init__(self) -> None:
        self.validate()
    
    def validate(self) -> None:
        if not self.rpc_endpoints:
            raise ValueError("At least one RPC endpoint is required")
        for endpoint in self.rpc_endpoints:
            if not endpoint.startswith(("wss://", "ws://")):
                raise ValueError(
                    f"RPC endpoint must be a WebSocket URL (ws:// or wss://): {endpoint}"
                )
        if not self.contracts:
            raise ValueError("At least one contract address is required")
        for contract in self.contracts:
            if not contract.startswith("0x") or len(contract) != 42:
                raise ValueError(f"Invalid contract address: {contract}")
        if self.ws_idle_timeout < 10:
            raise ValueError("ws_idle_timeout must be at least 10 seconds")
        if self.dedup_cache_size < 100:
            raise ValueError("dedup_cache_size must be at least 100")
        if self.token_cache_ttl < 60:
            raise ValueError("token_cache_ttl must be at least 60 seconds")
        if self.reconnect_jitter_max < 0:
            raise ValueError("reconnect_jitter_max must be non-negative")
    
    def validate_for_redis(self) -> None:
        if not self.redis_url:
            raise ValueError("redis_url is required for pub/sub mode")
