"""LRU cache for event deduplication."""

from collections import OrderedDict
from ..constants import Defaults


class LRUCache:
    """LRU cache using OrderedDict."""
    
    def __init__(self, max_size: int = Defaults.DEDUP_CACHE_SIZE) -> None:
        self._cache: OrderedDict[str, bool] = OrderedDict()
        self._max_size = max_size
    
    def add(self, key: str) -> bool:
        """Add key. Returns True if new, False if duplicate."""
        if key in self._cache:
            self._cache.move_to_end(key)
            return False
        self._cache[key] = True
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        return True
    
    def __contains__(self, key: str) -> bool:
        return key in self._cache
    
    def __len__(self) -> int:
        return len(self._cache)
    
    def clear(self) -> None:
        self._cache.clear()
