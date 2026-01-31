"""Event processor for OrderFilled events."""

from __future__ import annotations
import logging
import time
from typing import TYPE_CHECKING
from ..constants import Defaults
from ..models import OrderFilledEvent, ProcessorStats, Trade
from .cache import LRUCache

try:
    from eth_abi import decode
except ImportError:
    raise ImportError("eth-abi required: pip3 install eth-abi")

if TYPE_CHECKING:
    from collections.abc import Set

logger = logging.getLogger(__name__)


class EventProcessor:
    """Decodes and deduplicates OrderFilled events."""
    
    def __init__(self, dedup_cache_size: int = Defaults.DEDUP_CACHE_SIZE) -> None:
        self._processed_events = LRUCache(dedup_cache_size)
        self._events_received = 0
        self._events_processed = 0
        self._events_skipped = 0

    def process_event(
        self,
        log: dict,
        tracked_wallets: Set[str],
    ) -> list[Trade]:
        """Process event, return trades only for tracked wallets."""
        event = self._dedup_and_decode(log)
        if not event:
            return []

        if event.maker not in tracked_wallets:
            self._events_skipped += 1
            return []
        trade = self._create_trade_from_maker(event)
        self._events_processed += 1
        logger.info(
            f"Trade: {event.maker[:10]}... {trade.side} "
            f"${trade.size_usdc:.2f} @ {trade.price:.4f}"
        )
        return [trade]
    
    def process_event_all(self, log: dict) -> list[Trade]:
        """Process event for all wallets."""
        event = self._dedup_and_decode(log)
        if not event:
            return []
        trade = self._create_trade_from_maker(event)
        self._events_processed += 1
        return [trade]
    
    def _dedup_and_decode(self, log: dict) -> OrderFilledEvent | None:
        self._events_received += 1
        
        tx_hash = log.get("transactionHash", "")
        log_index = log.get("logIndex", "0x0")
        dedup_key = f"{tx_hash}:{log_index}"
        
        if not self._processed_events.add(dedup_key):
            self._events_skipped += 1
            return None
        
        event = self.decode_order_filled(log)
        if not event:
            self._events_skipped += 1
            return None
        
        return event

    def decode_order_filled(self, log: dict) -> OrderFilledEvent | None:
        try:
            topics = log.get("topics", [])
            data = log.get("data", "0x")
            if len(topics) < 4:
                return None
            order_hash_hex = topics[1]
            order_hash = bytes.fromhex(order_hash_hex.removeprefix("0x"))
            maker = "0x" + topics[2][-40:]
            taker = "0x" + topics[3][-40:]
            data_hex = data.removeprefix("0x")
            if len(data_hex) < 320:
                return None
            data_bytes = bytes.fromhex(data_hex)
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256"],
                data_bytes,
            )
            maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee = decoded
            return OrderFilledEvent(
                order_hash=order_hash,
                maker=maker.lower(),
                taker=taker.lower(),
                maker_asset_id=str(maker_asset_id),
                taker_asset_id=str(taker_asset_id),
                maker_amount_filled=maker_amount,
                taker_amount_filled=taker_amount,
                fee=fee,
                tx_hash=log.get("transactionHash", ""),
                block_number=int(log.get("blockNumber", "0x0"), 16),
                log_index=int(log.get("logIndex", "0x0"), 16),
            )
        except Exception as e:
            logger.debug(f"Failed to decode OrderFilled: {e}")
            return None
    
    def _create_trade_from_maker(self, event: OrderFilledEvent) -> Trade:
        if event.maker_asset_id == "0":
            side = "BUY"
            token_id = event.taker_asset_id
            size_usdc = event.maker_amount_filled / Defaults.DECIMAL_FACTOR
            size_shares = event.taker_amount_filled / Defaults.DECIMAL_FACTOR
        else:
            side = "SELL"
            token_id = event.maker_asset_id
            size_shares = event.maker_amount_filled / Defaults.DECIMAL_FACTOR
            size_usdc = event.taker_amount_filled / Defaults.DECIMAL_FACTOR
        price = size_usdc / size_shares if size_shares > 0 else 0.0
        return Trade(
            tx_hash=event.tx_hash,
            block_number=event.block_number,
            log_index=event.log_index,
            timestamp=int(time.time()),
            wallet=event.maker,
            token_id=token_id,
            side=side,
            price=price,
            size_usdc=size_usdc,
            size_shares=size_shares,
            is_maker=True,
        )
    
    def get_stats(self) -> ProcessorStats:
        return ProcessorStats(
            events_received=self._events_received,
            events_processed=self._events_processed,
            events_skipped=self._events_skipped,
            cache_size=len(self._processed_events),
        )
    
    def reset_stats(self) -> None:
        self._events_received = 0
        self._events_processed = 0
        self._events_skipped = 0
