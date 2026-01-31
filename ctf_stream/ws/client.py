"""WebSocket client for RPC log subscription."""

from __future__ import annotations
import asyncio
import json
import logging
import random
from typing import TYPE_CHECKING, AsyncIterator
from ..constants import ORDER_FILLED_TOPIC
from ..models import WebSocketStats

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError:
    raise ImportError(
        "websockets package required: pip install websockets"
    )

if TYPE_CHECKING:
    from websockets import WebSocketClientProtocol
    from ..config import Config

logger = logging.getLogger(__name__)

class WebSocketClient:
    """Subscribes to Polygon RPC logs with auto-reconnect."""
    
    def __init__(self, config: Config) -> None:
        self.config = config
        self.ws: WebSocketClientProtocol | None = None
        self.subscription_id: str | None = None
        self.connected = False
        self.current_endpoint_index = 0
        self.reconnect_count = 0
        self._endpoints = list(config.rpc_endpoints)
        self._reconnect_delays = list(config.reconnect_delays)
        self._contracts = list(config.contracts)
        self._running = False
        self._last_message_time = 0.0
        self._consecutive_timeouts = 0
        self._events_received_this_connection = 0
    
    @property
    def current_endpoint(self) -> str:
        return self._endpoints[self.current_endpoint_index % len(self._endpoints)]
    
    def _calculate_reconnect_delay(self, attempt: int) -> float:
        base_delay = self._reconnect_delays[min(attempt, len(self._reconnect_delays) - 1)]
        jitter = random.uniform(0, self.config.reconnect_jitter_max)
        return base_delay + jitter
    
    async def connect(self) -> bool:
        if not self._endpoints:
            logger.error("No RPC endpoints configured")
            return False
        start_index = self.current_endpoint_index % len(self._endpoints)
        for offset in range(len(self._endpoints)):
            i = (start_index + offset) % len(self._endpoints)
            endpoint = self._endpoints[i]
            try:
                logger.info(f"Connecting to {endpoint}...")
                self.ws = await websockets.connect(
                    endpoint,
                    ping_interval=self.config.ws_ping_interval,
                    ping_timeout=self.config.ws_ping_timeout,
                    close_timeout=5,
                )
                self.current_endpoint_index = i
                self.connected = True
                self._last_message_time = asyncio.get_event_loop().time()
                self._events_received_this_connection = 0
                logger.info(f"Connected to {endpoint}")
                return True
            except Exception as e:
                logger.warning(f"Failed to connect to {endpoint}: {e}")
                continue
        logger.error("All endpoints failed")
        return False
    
    async def subscribe_to_logs(self) -> bool:
        if not self.ws or not self.connected:
            logger.error("Cannot subscribe: not connected")
            return False
        subscription_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": self._contracts,
                    "topics": [ORDER_FILLED_TOPIC],
                }
            ]
        }
        try:
            logger.debug(f"Sending subscription request: {json.dumps(subscription_request)}")
            await self.ws.send(json.dumps(subscription_request))
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            data = json.loads(response)
            if "result" in data:
                self.subscription_id = data["result"]
                logger.info(
                    f"Subscribed to OrderFilled events on {len(self._contracts)} contracts. "
                    f"Subscription ID: {self.subscription_id}"
                )
                return True
            elif "error" in data:
                logger.error(f"Subscription error: {data['error']}")
                return False
            else:
                logger.warning(f"Unexpected subscription response: {data}")
                return False
        except asyncio.TimeoutError:
            logger.error("Subscription request timed out")
            return False
        except Exception as e:
            logger.error(f"Subscription failed: {e}")
            return False
    
    async def events(self) -> AsyncIterator[dict]:
        """Yield log events. Auto-reconnects on failure."""
        self._running = True
        while self._running:
            if not self.connected or not self.ws:
                success = await self._reconnect_with_backoff()
                if not success:
                    final_delay = self._calculate_reconnect_delay(len(self.config.reconnect_delays))
                    await asyncio.sleep(final_delay)
                    continue 
                if not await self.subscribe_to_logs():
                    await self.disconnect()
                    continue
            try:
                message = await asyncio.wait_for(
                    self.ws.recv(),
                    timeout=self.config.ws_idle_timeout,
                )
                self._last_message_time = asyncio.get_event_loop().time()
                self._consecutive_timeouts = 0
                data = json.loads(message)
                logger.debug(f"Received message: method={data.get('method')} keys={list(data.keys())}")
                if data.get("method") == "eth_subscription":
                    params = data.get("params", {})
                    if params.get("subscription") == self.subscription_id:
                        log_data = params.get("result", {})
                        self._events_received_this_connection += 1
                        yield log_data
                    else:
                        logger.warning(
                            f"Event for unknown subscription: {params.get('subscription')}"
                        )
            except asyncio.TimeoutError:
                self._consecutive_timeouts += 1
                logger.warning(
                    f"No messages received within timeout "
                    f"(consecutive: {self._consecutive_timeouts}, "
                    f"events this connection: {self._events_received_this_connection})"
                )
                
                if (
                    self._consecutive_timeouts >= self.config.max_timeouts_before_rotate
                    and self._events_received_this_connection == 0
                ):
                    old_endpoint = self.current_endpoint
                    self.current_endpoint_index = (
                        (self.current_endpoint_index + 1) % len(self._endpoints)
                    )
                    logger.warning(
                        f"RPC {old_endpoint} appears dead (connected but no events). "
                        f"Rotating to {self.current_endpoint}"
                    )
                    self._consecutive_timeouts = 0

                await self.disconnect()
                
            except ConnectionClosed as e:
                logger.warning(
                    f"WebSocket connection closed: code={e.code}, reason={e.reason}"
                )
                self.connected = False
                    
            except WebSocketException as e:
                logger.error(f"WebSocket error: {e}")
                self.connected = False
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received: {e}")
                continue
                
            except Exception as e:
                logger.error(f"Unexpected error in event loop: {e}", exc_info=True)
                self.connected = False
    
    async def _reconnect_with_backoff(self) -> bool:
        for attempt in range(len(self._reconnect_delays)):
            delay = self._calculate_reconnect_delay(attempt)
            logger.info(
                f"Reconnection attempt {attempt + 1}/{len(self._reconnect_delays)} in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)
            if await self.connect():
                self.reconnect_count += 1
                logger.info(
                    f"Reconnected successfully (total reconnects: {self.reconnect_count})"
                )
                return True
            self.current_endpoint_index = (self.current_endpoint_index + 1) % len(self._endpoints)
            logger.info(
                f"Connection failed, rotating to endpoint index {self.current_endpoint_index}"
            )
        logger.error("All reconnection attempts failed")
        return False
    
    async def disconnect(self) -> None:
        self.connected = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.debug(f"Error closing WebSocket: {e}")
            finally:
                self.ws = None
                self.subscription_id = None
    
    def stop(self) -> None:
        self._running = False
    
    def get_stats(self) -> WebSocketStats:
        last_message_age = None
        if self._last_message_time > 0:
            last_message_age = asyncio.get_event_loop().time() - self._last_message_time
        return WebSocketStats(
            connected=self.connected,
            endpoint=self.current_endpoint if self.connected else None,
            subscription_id=self.subscription_id,
            reconnect_count=self.reconnect_count,
            last_message_age=last_message_age,
            events_this_connection=self._events_received_this_connection,
            consecutive_timeouts=self._consecutive_timeouts,
        )
