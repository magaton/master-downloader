"""Single-domain spider. One instance runs per OS process (one per domain)."""

from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import scrapy
from scrapy import signals
from scrapy.exceptions import IgnoreRequest, StopDownload
from scrapy.http import HtmlResponse
from scrapy.linkextractors import LinkExtractor

from . import deadline
from .handler import DEADLINE_MSG, META_KEY, STARTED_KEY

HTML_TYPES = (b"text/html", b"application/xhtml")


def _write_file(path: Path, body: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(body)


class DomainSpider(scrapy.Spider):
    name = "domain"

    def __init__(self, start_url: str, out_dir: str, include_subdomains: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.start_url = start_url
        parsed = urlparse(start_url)
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        base = host[4:] if host.startswith("www.") else host
        self.domain = parsed.netloc
        # OffsiteMiddleware keeps us on this domain and its subdomains...
        self.allowed_domains = [base]
        # ...and unless include_subdomains is set, links are also limited to the
        # site itself (example.com and www.example.com), so e.g. forum.example.com
        # is not crawled. A startswith() check on a tuple of prefixes is much
        # cheaper than parsing every URL.
        self.url_prefixes: tuple[str, ...] | None = None
        if not include_subdomains:
            self.url_prefixes = tuple(
                f"{scheme}://{h}{end}"
                for scheme in ("https", "http")
                for h in (f"{base}{port}", f"www.{base}{port}")
                for end in ("/", "?")
            )

        # deny_extensions defaults to Scrapy's IGNORED_EXTENSIONS list (images,
        # video, audio, archives, office docs...), so those URLs are never
        # requested. canonicalize=True strips #fragments and sorts query args so
        # the same page is not found again under a different spelling.
        # (No allow_domains here: it compares against host:port and would reject
        # sites on a non-default port. The prefix check below and Scrapy's
        # OffsiteMiddleware keep the crawl on this site.)
        self.links = LinkExtractor(canonicalize=True, unique=True)
        self.seen: set[str] = {start_url}
        self.duplicate_links = 0

        self.out_dir = Path(out_dir)
        self.pages_dir = self.out_dir / "pages"
        # Create shard directories up front: no mkdir on the hot path, and no single
        # directory ends up with 100k files.
        for i in range(256):
            (self.pages_dir / f"{i:02x}").mkdir(parents=True, exist_ok=True)
        # Page bodies are written by a small thread pool so disk I/O never blocks
        # the event loop that drives the network.
        self.io = ThreadPoolExecutor(max_workers=4, thread_name_prefix="writer")
        self.report = open(self.out_dir / "pages.jsonl", "w", buffering=8192)  # small buffer so live progress is near real time

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.on_headers, signal=signals.headers_received)
        return spider

    # --- skip non-HTML as early as possible -------------------------------
    def on_headers(self, headers, body_length, request, spider):
        """Abort the transfer after the headers arrive if the body is not HTML.

        Links with media extensions are already filtered out. This catches the
        rest (e.g. /download?id=3 returning a PDF) before a single body byte is
        read, which frees the connection slot straight away.
        """
        if request.callback != self.parse:  # robots.txt and other internal requests
            return
        if b"Location" in headers:  # redirect: let it through
            return
        ctype = (headers.get(b"Content-Type") or b"").lower()
        if ctype and not ctype.startswith(HTML_TYPES):
            kind = ctype.split(b";")[0].decode("latin-1")
            self.crawler.stats.inc_value(f"fastcrawl/skipped_content_type/{kind}")
            raise StopDownload(fail=True)

    async def start(self):
        yield scrapy.Request(self.start_url, callback=self.parse, errback=self.on_error)

    def parse(self, response):
        if not isinstance(response, HtmlResponse):
            self.crawler.stats.inc_value("fastcrawl/skipped_non_html")
            return

        body = response.body
        digest = hashlib.sha1(response.url.encode()).hexdigest()
        path = self.pages_dir / digest[:2] / f"{digest}.html"
        self.io.submit(_write_file, path, body)

        seconds = response.meta.get(META_KEY) or response.meta.get("download_latency") or 1e-6
        size_kb = len(body) / 1024
        self.report.write(
            json.dumps(
                {
                    "domain": self.domain,
                    "url": response.url,
                    "size_kb": round(size_kb, 3),
                    "download_seconds": round(seconds, 6),
                    "speed_kb_s": round(size_kb / seconds, 3),
                    "location": str(path.resolve()),
                    "started_at": response.meta.get(STARTED_KEY),
                    "finished_at": time.time(),
                }
            )
            + "\n"
        )
        self.crawler.stats.inc_value("fastcrawl/pages")

        # After the time window closes, extracting links is wasted CPU.
        if deadline.expired():
            return
        seen, prefixes = self.seen, self.url_prefixes
        for link in self.links.extract_links(response):
            url = link.url
            if prefixes is not None and not url.startswith(prefixes):
                continue
            # Cheap in-process pre-filter: most links on a page (menus, footers)
            # were already seen. Skipping them here avoids creating a Request and
            # running it through the middleware chain. Scrapy's fingerprint
            # dupefilter is still the final check (e.g. for redirect targets).
            if url in seen:
                self.duplicate_links += 1
                continue
            seen.add(url)
            yield scrapy.Request(url, callback=self.parse, errback=self.on_error)

    def on_error(self, failure):
        if failure.check(StopDownload):
            self.crawler.stats.inc_value("fastcrawl/skipped_non_html")
        elif failure.check(IgnoreRequest) and DEADLINE_MSG in failure.getErrorMessage():
            self.crawler.stats.inc_value("fastcrawl/not_started_deadline")
        elif failure.check(IgnoreRequest) and "robots.txt" in failure.getErrorMessage():
            self.crawler.stats.inc_value("fastcrawl/blocked_by_robots")
        else:
            self.crawler.stats.inc_value("fastcrawl/errors")
            self.crawler.stats.inc_value(f"fastcrawl/errors/{failure.type.__name__}")
            self.logger.info("Failed %s: %s", failure.request.url, failure.getErrorMessage()[:200])

    def closed(self, reason):
        self.io.shutdown(wait=True)  # make sure every page is on disk
        self.report.close()
        self.crawler.stats.set_value("fastcrawl/duplicate_links_skipped", self.duplicate_links)
        stats = {k: v for k, v in self.crawler.stats.get_stats().items()}
        stats["close_reason"] = reason
        stats["window_closed_at"] = deadline.at()
        stats["window_close_reason"] = deadline.reason
        with open(self.out_dir / "stats.json", "w") as fh:
            json.dump(stats, fh, indent=2, default=str)
