"""Token ID to market metadata resolver."""

from __future__ import annotations
import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any
from ..constants import APIs, Defaults, RedisKeys
from ..models import MarketInfo, ResolverStats
try:
    import aiohttp
except ImportError:
    raise ImportError("aiohttp package required: pip install aiohttp")

if TYPE_CHECKING:
    from ..models import Trade

logger = logging.getLogger(__name__)

def _value_to_string(v: Any) -> str:
    return str(v).strip()


def _parse_json_field(field: str | list | None) -> list:
    if not field:
        return []
    if isinstance(field, str):
        try:
            return json.loads(field)
        except json.JSONDecodeError:
            return []
    return field

def _find_outcome_in_single_market(
    token_id: str,
    clob_ids: list[str],
    outcomes_raw: list,
) -> str:
    if token_id not in clob_ids:
        return ""
    idx = clob_ids.index(token_id)    
    if (
        isinstance(outcomes_raw, list)
        and len(outcomes_raw) == len(clob_ids)
        and (len(outcomes_raw) == 0 or not isinstance(outcomes_raw[0], dict))
    ):
        if idx < len(outcomes_raw):
            return _value_to_string(outcomes_raw[idx])
    return ""


def _find_outcome_in_multi_market(
    token_id: str,
    outcomes_raw: list[dict],
) -> tuple[str, str | None, str | None]:
    for sub in outcomes_raw:
        sub_clob_ids = [_value_to_string(x) for x in (sub.get("clobTokenIds") or [])]
        if token_id not in sub_clob_ids:
            continue
        sub_idx = sub_clob_ids.index(token_id)
        sub_outcomes = sub.get("outcomes") or []
        outcome = ""
        if isinstance(sub_outcomes, list) and sub_idx < len(sub_outcomes):
            outcome = _value_to_string(sub_outcomes[sub_idx])
        title = sub.get("question") or sub.get("title")
        condition_id = sub.get("conditionId")
        return outcome, title, condition_id
    return "", None, None

class TokenResolver:
    """Resolves token IDs: memory → Redis → Gamma API."""
    
    def __init__(
        self,
        cache_ttl: int = Defaults.TOKEN_CACHE_TTL,
        redis_url: str | None = None,
        api_timeout: float = Defaults.API_TIMEOUT,
    ) -> None:
        self._cache: dict[str, MarketInfo] = {}
        self._cache_ttl = cache_ttl
        self._redis_url = redis_url
        self._api_timeout = api_timeout
        self._session: aiohttp.ClientSession | None = None
        self._redis: Any = None
        self._memory_hits = 0
        self._redis_hits = 0
        self._api_hits = 0
        self._api_errors = 0
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=self._api_timeout,
                    connect=min(1.0, self._api_timeout / 2),
                ),
                headers={"User-Agent": "ActivityStream-SDK/0.1"},
            )
        return self._session
    
    async def _get_redis(self) -> Any | None:
        if not self._redis_url:
            return None
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                
                self._redis = aioredis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                )
                await self._redis.ping()
            except ImportError:
                logger.warning(
                    "redis package not installed. Install with: pip3 install redis"
                )
                self._redis_url = None
                return None
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
                self._redis = None
        return self._redis
    
    async def _lookup_redis(self, token_id: str) -> MarketInfo | None:
        try:
            r = await self._get_redis()
            if not r:
                return None            
            token_index_key = f"{RedisKeys.TOKEN_INDEX_PREFIX}{token_id}"
            index_data = await r.hgetall(token_index_key)
            if index_data and index_data.get("slug"):
                slug = index_data["slug"]
                market_cache_key = f"{RedisKeys.MARKET_CACHE_PREFIX}{slug}"
                market_data = await r.hgetall(market_cache_key)
                
                if market_data:
                    info = self._parse_market_data(token_id, slug, market_data)
                    if info:
                        self._redis_hits += 1
                        return info
            return None
        except Exception as e:
            logger.debug(f"Redis lookup failed for {token_id}: {e}")
            return None
    
    def _parse_market_data(
        self,
        token_id: str,
        slug: str,
        market_data: dict,
    ) -> MarketInfo | None:
        try:
            token_id = str(token_id).strip()
            clob_ids_raw = _parse_json_field(market_data.get("clobTokenIds"))
            outcomes_raw = _parse_json_field(market_data.get("outcomes"))
            clob_ids = [_value_to_string(x) for x in clob_ids_raw]
            outcome = _find_outcome_in_single_market(token_id, clob_ids, outcomes_raw)
            resolved_title = None
            resolved_condition_id = None
            
            if not outcome and outcomes_raw and isinstance(outcomes_raw[0], dict):
                outcome, resolved_title, resolved_condition_id = _find_outcome_in_multi_market(
                    token_id, outcomes_raw
                )
            title = (
                resolved_title
                or market_data.get("question")
                or market_data.get("title")
                or "Unknown Market"
            )
            return MarketInfo(
                token_id=token_id,
                market_slug=slug,
                event_slug=slug,
                outcome=outcome,
                title=title,
                condition_id=resolved_condition_id or market_data.get("conditionId") or market_data.get("condition_id"),
            )
        except Exception as e:
            logger.debug(f"Failed to parse market data: {e}")
            return None
    
    async def resolve(self, token_id: str) -> MarketInfo | None:
        if token_id in self._cache:
            cached = self._cache[token_id]
            if not cached.is_expired(self._cache_ttl):
                self._memory_hits += 1
                return cached
            else:
                del self._cache[token_id]
        
        redis_result = await self._lookup_redis(token_id)
        if redis_result:
            self._cache[token_id] = redis_result
            logger.debug(
                f"Redis hit for token {token_id[:20]}... "
                f"→ slug={redis_result.market_slug}"
            )
            return redis_result

        self._api_hits += 1
        return await self._fetch_from_gamma(token_id)
    
    async def _fetch_from_gamma(self, token_id: str) -> MarketInfo | None:
        try:
            session = await self._get_session()
            url = f"{APIs.GAMMA_MARKETS_ENDPOINT}?clob_token_ids={token_id}"
            
            async with session.get(url) as response:
                if response.status != 200:
                    logger.warning(
                        f"Gamma API returned {response.status} for token {token_id}"
                    )
                    self._api_errors += 1
                    return None
                
                data = await response.json()
                markets = data.get("value", data) if isinstance(data, dict) else data
                
                if not markets:
                    logger.warning(f"No market found for token {token_id}")
                    return None
                
                market = markets[0]
                clob_ids = [_value_to_string(x) for x in _parse_json_field(market.get("clobTokenIds"))]
                outcomes_list = _parse_json_field(market.get("outcomes"))
                outcome = _find_outcome_in_single_market(token_id, clob_ids, outcomes_list)
                yes_token_id = clob_ids[0] if len(clob_ids) >= 1 else None
                no_token_id = clob_ids[1] if len(clob_ids) >= 2 else None
                event_slug = ""
                events = market.get("events", [])
                if events:
                    event_slug = events[0].get("slug", "")
                info = MarketInfo(
                    token_id=token_id,
                    market_slug=market.get("slug", market.get("market_slug", "")),
                    event_slug=event_slug,
                    outcome=outcome or market.get("outcome", ""),
                    title=market.get("question", market.get("title", "Unknown Market")),
                    condition_id=market.get("conditionId"),
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    outcomes=outcomes_list if outcomes_list else None,
                )
                self._cache[token_id] = info
                
                await self._cache_to_redis(info)
                
                logger.debug(f"Gamma API hit for token {token_id[:20]}... → {info.market_slug}")
                return info
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout resolving token {token_id}")
            self._api_errors += 1
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"HTTP error resolving token {token_id}: {e}")
            self._api_errors += 1
            return None
        except Exception as e:
            logger.error(f"Unexpected error resolving token {token_id}: {e}")
            self._api_errors += 1
            return None
    
    async def _cache_to_redis(self, info: MarketInfo) -> None:
        try:
            r = await self._get_redis()
            if not r:
                return
            
            ttl = Defaults.REDIS_CACHE_TTL
            token_index_key = f"{RedisKeys.TOKEN_INDEX_PREFIX}{info.token_id}"
            await r.hset(token_index_key, mapping={"slug": info.market_slug or info.event_slug})
            await r.expire(token_index_key, ttl)
        except Exception as e:
            logger.debug(f"Failed to cache to Redis: {e}")
    
    async def enrich_trade(self, trade: Trade) -> None:
        info = await self.resolve(trade.token_id)
        if info:
            trade.market_slug = info.market_slug
            trade.market_title = info.title
            trade.outcome = info.outcome
            trade.event_slug = info.event_slug
            trade.condition_id = info.condition_id
            logger.debug(
                f"Enriched trade: token={trade.token_id[:20]}... → "
                f"outcome={info.outcome}, market={info.market_slug}"
            )
        else:
            logger.warning(f"Could not resolve token_id: {trade.token_id}")
    
    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        if self._redis:
            await self._redis.close()
    
    def get_stats(self) -> ResolverStats:
        return ResolverStats(
            memory_cache_size=len(self._cache),
            memory_hits=self._memory_hits,
            redis_hits=self._redis_hits,
            api_hits=self._api_hits,
            api_errors=self._api_errors,
        )
    
    def clear_cache(self) -> None:
        self._cache.clear()
        logger.info("Token memory cache cleared")
