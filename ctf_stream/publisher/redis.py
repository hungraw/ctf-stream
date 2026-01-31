"""Provides optional Redis pub/sub publishing."""

from __future__ import annotations
import json
import logging
from typing import TYPE_CHECKING, Any
from ..constants import RedisKeys

if TYPE_CHECKING:
    from ..models import Trade

logger = logging.getLogger(__name__)

class RedisPublisher:
    """
    Publishes trades to Redis pub/sub channels.
    Channels:
      activity:trades
    """
    def __init__(self, redis_url: str) -> None:
        """Initialize Redis publisher."""
        self._redis_url = redis_url
        self._redis: Any = None
        self._connected = False
        self._publishes = 0
        self._errors = 0
    
    async def connect(self) -> bool:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                self._redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            self._connected = True
            logger.info("Connected to Redis")
            return True
        except ImportError:
            logger.error("redis package not installed. Install with: pip3 install redis")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
    
    async def publish(self, trade: Trade) -> bool:
        """Publish a trade to Redis."""
        if not self._redis:
            logger.warning("Cannot publish: not connected to Redis")
            self._errors += 1
            return False
        try:
            payload = json.dumps(trade.to_dict())
            await self._redis.publish(RedisKeys.CHANNEL_TRADES, payload)
            self._publishes += 1
            return True
        except Exception as e:
            logger.error(f"Failed to publish trade: {e}")
            self._errors += 1
            return False
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None
        self._connected = False
    
    def get_stats(self) -> dict[str, Any]:
        """Get publisher statistics."""
        return {
            "connected": self._connected,
            "publishes": self._publishes,
            "errors": self._errors,
        }
