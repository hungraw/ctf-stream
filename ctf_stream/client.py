"""ActivityStream client."""

from __future__ import annotations
import asyncio
import logging
from typing import TYPE_CHECKING, AsyncIterator
from .config import Config
from .models import ServiceStats, Trade
from .processor import EventProcessor
from .resolver import TokenResolver
from .ws import WebSocketClient

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ActivityClient:
    """Stream trades from blockchain."""
    
    def __init__(
        self,
        rpc_endpoints: list[str] | None = None,
        redis_url: str | None = None,
        config: Config | None = None,
        **kwargs,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            if rpc_endpoints is None:
                # Use defaults from Config
                self._config = Config(redis_url=redis_url, **kwargs)
            else:
                self._config = Config(
                    rpc_endpoints=rpc_endpoints,
                    redis_url=redis_url,
                    **kwargs,
                )
        
        self._ws_client: WebSocketClient | None = None
        self._processor: EventProcessor | None = None
        self._resolver: TokenResolver | None = None
        self._tracked_wallets: set[str] = set()
        self._track_all = False
        self._running = False
        self._start_time: float | None = None
    
    @property
    def config(self) -> Config:
        return self._config
    
    @property
    def tracked_wallets(self) -> set[str]:
        return self._tracked_wallets.copy()
    
    async def track_wallet(self, address: str) -> None:
        self._tracked_wallets.add(address.lower())
        logger.info(f"Tracking wallet: {address[:10]}... (total: {len(self._tracked_wallets)})")
    
    async def untrack_wallet(self, address: str) -> None:
        self._tracked_wallets.discard(address.lower())
        logger.info(f"Untracked wallet: {address[:10]}... (total: {len(self._tracked_wallets)})")
    
    async def track_wallets(self, addresses: list[str]) -> None:
        for address in addresses:
            self._tracked_wallets.add(address.lower())
        logger.info(f"Tracking {len(addresses)} wallets (total: {len(self._tracked_wallets)})")
    
    def set_track_all(self, enabled: bool = True) -> None:
        self._track_all = enabled
        mode = "all trades" if enabled else "tracked wallets only"
        logger.info(f"Track mode: {mode}")
    
    async def stream_trades(
        self,
        enrich: bool = True,
    ) -> AsyncIterator[Trade]:
        """Stream trades. Set enrich=False to skip metadata resolution."""
        self._running = True
        self._ws_client = WebSocketClient(self._config)
        self._processor = EventProcessor(self._config.dedup_cache_size)
        if enrich:
            self._resolver = TokenResolver(
                cache_ttl=self._config.token_cache_ttl,
                redis_url=self._config.redis_url,
                api_timeout=self._config.api_timeout,
            )
        self._start_time = asyncio.get_event_loop().time()
        try:
            async for log_event in self._ws_client.events():
                if not self._running:
                    break
                if self._track_all:
                    trades = self._processor.process_event_all(log_event)
                else:
                    trades = self._processor.process_event(
                        log_event,
                        self._tracked_wallets,
                    )
                for trade in trades:
                    if enrich and self._resolver:
                        await self._resolver.enrich_trade(trade)
                    yield trade
        except asyncio.CancelledError:
            logger.info("Stream cancelled")
            raise
        finally:
            await self._cleanup()
    
    async def _cleanup(self) -> None:
        if self._ws_client:
            await self._ws_client.disconnect()
            self._ws_client = None
        if self._resolver:
            await self._resolver.close()
            self._resolver = None
    
    async def stop(self) -> None:
        self._running = False
        if self._ws_client:
            self._ws_client.stop()
    
    def get_stats(self) -> ServiceStats:
        uptime = 0.0
        if self._start_time:
            uptime = asyncio.get_event_loop().time() - self._start_time
        ws_stats = self._ws_client.get_stats() if self._ws_client else None
        proc_stats = self._processor.get_stats() if self._processor else None
        res_stats = self._resolver.get_stats() if self._resolver else None     
        from .models import ProcessorStats, ResolverStats, WebSocketStats
        if not ws_stats:
            ws_stats = WebSocketStats(
                connected=False,
                endpoint=None,
                subscription_id=None,
                reconnect_count=0,
                last_message_age=None,
                events_this_connection=0,
                consecutive_timeouts=0,
            )
        if not proc_stats:
            proc_stats = ProcessorStats(
                events_received=0,
                events_processed=0,
                events_skipped=0,
                cache_size=0,
            )
        if not res_stats:
            res_stats = ResolverStats(
                memory_cache_size=0,
                memory_hits=0,
                redis_hits=0,
                api_hits=0,
                api_errors=0,
            )
        return ServiceStats(
            status="running" if self._running else "stopped",
            uptime_seconds=uptime,
            websocket=ws_stats,
            processor=proc_stats,
            resolver=res_stats,
            tracked_wallets=len(self._tracked_wallets),
        )
    
    async def __aenter__(self) -> ActivityClient:
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        await self._cleanup()
