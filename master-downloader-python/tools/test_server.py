"""Local test websites for checking master-downloader-python's requirements.

Starts 4 websites on 127.0.0.1:8001-8004 (standard library only). Each site is an
endless tree of HTML pages, and every page:

* is exactly --size KB, like the task's "all pages have the same size" assumption;
* takes --delay seconds to download. The body is streamed in slices, so at any
  moment many downloads are *in flight*, which makes the graceful stop visible;
* links to 10 child pages (so the crawl never runs out), plus traps the
  crawler must handle:
    - the home page, its parent and itself#fragment -> duplicates, must be skipped
    - /img/N.jpg and /video/N.mp4                   -> media, must never be requested
    - /download/N (no extension, serves a PDF)      -> must be aborted after headers
    - /private/N (forbidden in robots.txt)          -> must never be requested

The server counts what it serves and prints a check every 5 s and on Ctrl-C. It is
an independent check of the crawler's own report:

    duplicate page requests : 0   <- every page downloaded only once
    media requests          : 0   <- images and videos ignored
    private requests        : 0   <- robots.txt respected
    PDF downloads aborted   : n   <- non-HTML skipped without reading the body

Run:  uv run python tools/test_server.py --delay 2
Then: uv run master-downloader-python -c config.local.toml
"""

from __future__ import annotations

import argparse
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOREM = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor "
    "incididunt ut labore et dolore magna aliqua. "
)

stats: Counter = Counter()
seen_pages: dict[int, set[str]] = {}
lock = threading.Lock()


def make_handler(port: int, size_kb: int, delay: float):
    padding = (LOREM * (size_kb * 1024 // len(LOREM) + 1)).encode()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive, so connection reuse can be tested

        def log_message(self, *args):  # keep the console readable
            pass

        def _send(self, status: int, ctype: str, body: bytes, slices: int = 10) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                if delay <= 0 or slices == 1:
                    self.wfile.write(body)
                    return
                step = max(1, len(body) // slices)
                for i in range(0, len(body), step):
                    time.sleep(delay / slices)
                    self.wfile.write(body[i:i + step])
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                with lock:
                    stats["transfers aborted by client"] += 1
                    if ctype == "application/pdf":
                        stats["PDF downloads aborted"] += 1

        def do_GET(self):
            path = self.path.split("#")[0]
            if path == "/robots.txt":
                return self._send(200, "text/plain", b"User-agent: *\nDisallow: /private/\n", slices=1)
            if path.startswith(("/img/", "/video/")):
                with lock:
                    stats["media requests"] += 1
                return self._send(200, "image/jpeg", b"\xff\xd8" + b"0" * 50_000)
            if path.startswith("/private/"):
                with lock:
                    stats["private requests"] += 1
                return self._send(200, "text/html", b"<html>private</html>")
            if path.startswith("/download/"):
                with lock:
                    stats["PDF requests"] += 1
                return self._send(200, "application/pdf", b"%PDF-1.4" + b"0" * 1_000_000, slices=100)

            if path == "/":
                page = 0
            elif path.startswith("/p/") and path[3:].isdigit():
                page = int(path[3:])
            else:
                return self._send(404, "text/html", b"<html>not found</html>", slices=1)

            with lock:
                site = seen_pages.setdefault(port, set())
                if path in site:
                    stats["duplicate page requests"] += 1
                site.add(path)
                stats["HTML pages served"] += 1

            children = "".join(f'<li><a href="/p/{page * 10 + i}">page {page * 10 + i}</a></li>'
                               for i in range(1, 11))
            parent = max(0, (page - 1) // 10)
            html = (
                f"<!doctype html><html><head><title>Site {port} page {page}</title></head><body>"
                f"<h1>Site {port} / page {page}</h1>"
                f'<nav><a href="/">home</a> <a href="/p/{parent}">parent</a> '
                f'<a href="/p/{page}#top">this page again</a></nav>'
                f"<ul>{children}</ul>"
                f'<img src="/img/{page}.jpg"><a href="/img/{page}.jpg">photo</a> '
                f'<a href="/video/{page}.mp4">video</a> '
                f'<a href="/download/{page}">report (PDF)</a> '
                f'<a href="/private/{page}">private</a>'
                "<p>"
            ).encode()
            tail = b"</p></body></html>"
            body = html + padding[: max(0, size_kb * 1024 - len(html) - len(tail))] + tail
            self._send(200, "text/html; charset=utf-8", body)

    return Handler


def print_stats() -> None:
    with lock:
        snapshot = dict(stats)
    keys = ["HTML pages served", "duplicate page requests", "media requests", "private requests",
            "PDF requests", "PDF downloads aborted"]
    print("  " + " | ".join(f"{k}: {snapshot.get(k, 0)}" for k in keys), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ports", type=int, nargs="+", default=[8001, 8002, 8003, 8004])
    ap.add_argument("--delay", type=float, default=2.0, help="seconds each page takes to download")
    ap.add_argument("--size", type=int, default=30, help="page size in KB")
    a = ap.parse_args()

    for port in a.ports:
        srv = ThreadingHTTPServer(("127.0.0.1", port), make_handler(port, a.size, a.delay))
        srv.daemon_threads = True
        srv.request_queue_size = 1024
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"Serving {len(a.ports)} test sites: "
          + ", ".join(f"http://127.0.0.1:{p}/" for p in a.ports)
          + f"  (page size {a.size} KB, {a.delay}s per page). Ctrl-C to stop.", flush=True)
    try:
        while True:
            time.sleep(5)
            print_stats()
    except KeyboardInterrupt:
        print("\nFinal:")
        print_stats()


if __name__ == "__main__":
    main()
