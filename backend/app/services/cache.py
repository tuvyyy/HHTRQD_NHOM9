# app/services/cache.py
import time
import threading
from typing import Any, Optional

class TTLCache:
    def __init__(self, default_ttl: int = 600, max_items: int = 2000):
        self.default_ttl = default_ttl
        self.max_items = max_items
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def _cleanup(self):
        now = time.time()
        expired = [k for k, (exp, _) in self._data.items() if exp <= now]
        for k in expired:
            self._data.pop(k, None)

        # nếu quá max_items thì drop bớt (FIFO-ish theo thời gian hết hạn)
        if len(self._data) > self.max_items:
            items = sorted(self._data.items(), key=lambda kv: kv[1][0])  # theo exp
            for k, _ in items[: len(self._data) - self.max_items]:
                self._data.pop(k, None)

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            self._cleanup()
            v = self._data.get(key)
            if not v:
                return None
            exp, data = v
            if exp <= time.time():
                self._data.pop(key, None)
                return None
            return data

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        with self._lock:
            self._cleanup()
            exp = time.time() + (ttl if ttl is not None else self.default_ttl)
            self._data[key] = (exp, value)

cache = TTLCache(default_ttl=600, max_items=2000)  # cache 10 phút
