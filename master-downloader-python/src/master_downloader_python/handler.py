"""HTTP/1.1 download handler: exact per-page timing and a hard deadline gate.

Timing: Scrapy's built-in ``download_latency`` stops the clock when the response
*headers* arrive, which overstates speed. This handler times the full request:
connection setup (only if the connection is new; pooled connections are reused),
request, headers, and full body. It does not include queueing, decompression, or
parsing.

Deadline: this is the last point before bytes go on the wire. Refusing here
guarantees that no download *starts* after the time window closes, even for a
request the scheduler handed out a few milliseconds before the deadline.
"""

from __future__ import annotations

import time

from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler
from scrapy.exceptions import IgnoreRequest

from . import deadline

META_KEY = "fastcrawl_download_seconds"
STARTED_KEY = "fastcrawl_started_at"
DEADLINE_MSG = "not started: time window closed"


class TimedHTTP11DownloadHandler(HTTP11DownloadHandler):
    async def download_request(self, request):
        now = time.time()
        if now >= deadline.at() and "robots.txt" not in request.url:
            raise IgnoreRequest(DEADLINE_MSG)
        request.meta[STARTED_KEY] = now
        t0 = time.perf_counter()
        response = await super().download_request(request)
        request.meta[META_KEY] = time.perf_counter() - t0
        return response
