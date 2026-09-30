"""The moment after which no new download may start (one per process).

The deadline starts as "window start + duration". Ctrl-C or SIGTERM moves it
to *now*, so an early stop takes exactly the same graceful path as the normal
end of the time window: no new downloads, in-flight downloads finish.
"""

from __future__ import annotations

import time

_at: float = float("inf")
reason: str = "time window closed"


def at() -> float:
    return _at


def set_at(ts: float) -> None:
    global _at
    _at = ts


def expired() -> bool:
    return time.time() >= _at


def expire_now(why: str) -> bool:
    """Close the window now. Returns False if it was already closed."""
    global _at, reason
    now = time.time()
    if now >= _at:
        return False
    _at, reason = now, why
    return True
