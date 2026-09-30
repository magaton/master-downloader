"""Scheduler that enforces the crawl time window.

After the deadline the scheduler reports "nothing pending" and hands out no
more requests, so **no new download can start**. Requests already handed to
the downloader are not touched, so they finish normally. Once they are done
the engine sees an idle spider and shuts down cleanly. This is a graceful stop
without cancelling anything that is in flight.
"""

from __future__ import annotations

import logging

from scrapy.core.scheduler import Scheduler

from . import deadline

logger = logging.getLogger(__name__)


class DeadlineScheduler(Scheduler):
    _expired_logged: bool = False

    @classmethod
    def from_crawler(cls, crawler):
        scheduler = super().from_crawler(crawler)
        scheduler.stats_ref = crawler.stats
        return scheduler

    def _expired(self) -> bool:
        if not deadline.expired():
            return False
        if not self._expired_logged:
            self._expired_logged = True
            discarded = len(self)
            self.stats_ref.set_value("fastcrawl/discarded_at_deadline", discarded)
            logger.info(
                "%s: no new downloads; %d queued requests discarded, "
                "waiting for in-flight downloads to finish",
                deadline.reason[:1].upper() + deadline.reason[1:], discarded,
            )
        return True

    def has_pending_requests(self) -> bool:
        return False if self._expired() else super().has_pending_requests()

    def next_request(self):
        return None if self._expired() else super().next_request()

    def enqueue_request(self, request) -> bool:
        # Links found in pages that finish after the deadline are simply dropped.
        return False if self._expired() else super().enqueue_request(request)
