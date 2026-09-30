"""Scrapy settings tuned for raw page throughput."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def build_settings(*, concurrency: int, download_timeout: float, obey_robots: bool,
                   out_dir: Path) -> dict:
    s = {
        "BOT_NAME": "fastcrawl",
        "USER_AGENT": "Mozilla/5.0 (compatible; fastcrawl/0.1)",
        "ROBOTSTXT_OBEY": obey_robots,

        # --- concurrency ---------------------------------------------------
        # One domain per process, so the global and per-domain limits are equal.
        # Every request the engine takes from the scheduler then goes straight
        # onto a connection instead of waiting in a per-domain queue, which also
        # makes the deadline check exact.
        "CONCURRENT_REQUESTS": concurrency,
        "CONCURRENT_REQUESTS_PER_DOMAIN": concurrency,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,

        # --- don't let slow or broken pages hold connection slots ----------
        "DOWNLOAD_TIMEOUT": download_timeout,
        "RETRY_TIMES": 1,
        "REDIRECT_MAX_TIMES": 5,
        "DOWNLOAD_MAXSIZE": 10 * 1024 * 1024,
        "DOWNLOAD_WARNSIZE": 0,

        # --- network -------------------------------------------------------
        # HTTP/1.1 keep-alive pool (the handler sizes it to the per-domain
        # concurrency), gzip/deflate (+ br/zstd if installed), and a DNS cache.
        "COMPRESSION_ENABLED": True,
        "DNSCACHE_ENABLED": True,
        "DNSCACHE_SIZE": 10_000,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
            "Accept-Language": "en",
        },
        "DOWNLOAD_HANDLERS": {
            "http": "master_downloader_python.handler.TimedHTTP11DownloadHandler",
            "https": "master_downloader_python.handler.TimedHTTP11DownloadHandler",
        },

        # --- scheduling ----------------------------------------------------
        # Breadth-first order spreads requests over many pages, so the queue
        # never runs dry and all connection slots stay busy.
        "SCHEDULER": "master_downloader_python.scheduler.DeadlineScheduler",
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",

        # --- turn off work that isn't needed ------------------------------
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "MEMUSAGE_ENABLED": False,
        "HTTPCACHE_ENABLED": False,
        "SPIDER_MIDDLEWARES": {
            "scrapy.spidermiddlewares.referer.RefererMiddleware": None,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware": None,
        },

        # --- logging (per-domain file; the console gets a live summary) -----
        "LOG_LEVEL": "INFO",
        "LOG_FILE": str(out_dir / "crawl.log"),
        "LOGSTATS_INTERVAL": 10,
    }

    # uvloop makes the asyncio event loop that drives the Twisted reactor
    # faster (Linux/macOS).
    if importlib.util.find_spec("uvloop") is not None:
        s["ASYNCIO_EVENT_LOOP"] = "uvloop.Loop"
    return s
