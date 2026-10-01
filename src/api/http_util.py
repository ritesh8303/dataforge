"""HTTP helpers: CORS origin list + cheap in-process rate limit."""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque


def allowed_origins() -> list[str]:
    raw = os.environ.get("ALLOWED_ORIGIN", "*").strip()
    if not raw or raw == "*":
        return ["*"]
    return [p.strip() for p in raw.split(",") if p.strip()]


_HITS: dict[str, deque[float]] = defaultdict(deque)


def rate_limit_ok(key: str, *, limit: int = 30, window_s: float = 60.0) -> bool:
    """True if this key is under the sliding-window cap."""
    now = time.time()
    q = _HITS[key]
    while q and now - q[0] > window_s:
        q.popleft()
    if len(q) >= limit:
        return False
    q.append(now)
    return True
