"""Constants for ActivityStream."""

from typing import Final

CTF_EXCHANGE: Final[str] = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE: Final[str] = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER: Final[str] = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
DEFAULT_CONTRACTS: Final[tuple[str, ...]] = (
    CTF_EXCHANGE,
    NEG_RISK_CTF_EXCHANGE,
    NEG_RISK_ADAPTER,
)

ORDER_FILLED_TOPIC: Final[str] = (
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)

class RedisKeys:
    """Redis key prefixes and channel names."""
    CHANNEL_TRADES: Final[str] = "activity:trades"
    CHANNEL_COPY_TRADES: Final[str] = "activity:copy_trades"
    CHANNEL_HEARTBEAT: Final[str] = "activity:heartbeat"
    CHANNEL_WALLET_UPDATES: Final[str] = "activity:wallet_updates"
    TRACKED_WALLETS: Final[str] = "activity:tracked_wallets"
    COPIED_LEADERS: Final[str] = "activity:copied_leaders"
    TOKEN_INDEX_PREFIX: Final[str] = "token_index:"
    MARKET_CACHE_PREFIX: Final[str] = "market_cache:"

class Defaults:
    """Default configuration values."""
    PING_INTERVAL: Final[int] = 20
    PING_TIMEOUT: Final[int] = 10
    CLOSE_TIMEOUT: Final[int] = 5
    IDLE_TIMEOUT: Final[int] = 60
    
    # Reconnection
    RECONNECT_DELAYS: Final[tuple[int, ...]] = (1, 2, 4, 8, 16, 30, 60)
    RECONNECT_JITTER_MAX: Final[float] = 1.0  
    MAX_TIMEOUTS_BEFORE_ROTATE: Final[int] = 2
    
    # Caching
    TOKEN_CACHE_TTL: Final[int] = 3600
    DEDUP_CACHE_SIZE: Final[int] = 1000
    REDIS_CACHE_TTL: Final[int] = 7 * 24 * 3600
    
    # API
    API_TIMEOUT: Final[float] = 2.0
    API_CONNECT_TIMEOUT: Final[float] = 1.0
    
    # Token precision
    DECIMALS: Final[int] = 6
    DECIMAL_FACTOR: Final[int] = 10 ** DECIMALS

class APIs:
    """External API endpoints."""
    GAMMA_API_BASE: Final[str] = "https://gamma-api.polymarket.com"
    GAMMA_MARKETS_ENDPOINT: Final[str] = f"{GAMMA_API_BASE}/markets"

class RPCEndpoints:
    """
    Public Polygon RPC endpoints.
    Note: Using multiple providers is recommended to avoid rate limiting.
    """
    PUBLICNODE: Final[str] = "wss://polygon-bor-rpc.publicnode.com"
    DRPC: Final[str] = "wss://polygon.drpc.org"
    
    # Default endpoint list
    DEFAULT_ENDPOINTS: Final[tuple[str, ...]] = (
        PUBLICNODE,
        DRPC,
    )
