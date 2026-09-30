"""Entry point: one crawler process per domain, live progress, final report."""

from __future__ import annotations

import argparse
import csv
import json
import multiprocessing as mp
import os
import signal
import statistics
import sys
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


@dataclass
class Config:
    domains: list[str]
    duration_seconds: float = 60
    output_dir: str = "output"
    concurrency_per_domain: int = 64
    download_timeout: float = 15
    obey_robots: bool = True
    pin_cpu: bool = True
    include_subdomains: bool = False


def normalise_start_url(d: str) -> str:
    d = d.strip()
    if "://" not in d:
        d = f"https://{d}/"
    if not urlparse(d).hostname:
        raise SystemExit(f"Invalid domain/URL in config: {d!r}")
    return d


def load_config(argv: list[str] | None) -> Config:
    p = argparse.ArgumentParser(prog="master-downloader-python", description=__doc__)
    p.add_argument("-c", "--config", default="config.toml", help="TOML config file (default: config.toml)")
    p.add_argument("-d", "--domains", nargs="+", help="override the domains/start URLs from the config")
    p.add_argument("-t", "--duration", type=float, help="override duration_seconds")
    p.add_argument("--concurrency", type=int, help="override concurrency_per_domain")
    p.add_argument("--output", help="override output_dir")
    p.add_argument("--no-robots", action="store_true", help="ignore robots.txt")
    a = p.parse_args(argv)

    data: dict = {}
    if Path(a.config).exists():
        data = tomllib.loads(Path(a.config).read_text())
    elif a.config != "config.toml" or not a.domains:
        raise SystemExit(f"Config file not found: {a.config}")

    known = Config.__dataclass_fields__.keys()
    unknown = set(data) - set(known)
    if unknown:
        raise SystemExit(f"Unknown config keys: {sorted(unknown)}")
    cfg = Config(**{"domains": [], **data})
    if a.domains:
        cfg.domains = a.domains
    if a.duration is not None:
        cfg.duration_seconds = a.duration
    if a.concurrency is not None:
        cfg.concurrency_per_domain = a.concurrency
    if a.output:
        cfg.output_dir = a.output
    if a.no_robots:
        cfg.obey_robots = False

    cfg.domains = [normalise_start_url(d) for d in cfg.domains]
    if not cfg.domains:
        raise SystemExit("No domains configured")
    if len(cfg.domains) != 4:
        print(f"note: {len(cfg.domains)} domains configured (the task expects 4); "
              f"running one process per domain anyway", file=sys.stderr)
    if len({urlparse(d).netloc for d in cfg.domains}) != len(cfg.domains):
        raise SystemExit("Each domain must be different")
    return cfg


# --------------------------------------------------------------------------
# child process
# --------------------------------------------------------------------------
def run_domain(start_url: str, out_dir: str, settings_kwargs: dict, cpu: int | None,
               include_subdomains: bool, ready, go, shared_deadline) -> None:
    # Pin the process to one core: no migrations between cores (warm caches),
    # and 4 domains map exactly onto the 4 cores.
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        try:
            os.sched_setaffinity(0, {cpu})
        except OSError:
            pass
    # Do the slow imports *before* the clock starts, then wait until every
    # process is ready so that all domains get the same time window.
    from scrapy.crawler import CrawlerProcess

    from .settings import build_settings
    from .spider import DomainSpider

    from . import deadline as window

    ready.wait()
    go.wait()
    window.set_at(shared_deadline.value)
    _install_stop_handlers()
    process = CrawlerProcess(build_settings(**settings_kwargs))
    process.crawl(DomainSpider, start_url=start_url, out_dir=out_dir,
                  include_subdomains=include_subdomains)
    # Scrapy's own Ctrl-C handling is replaced by ours (below).
    process.start(install_signal_handlers=False)


def _install_stop_handlers() -> None:
    """Ctrl-C / SIGTERM = close the time window now, then shut down gracefully.

    The first signal moves the deadline to "now": no new downloads start, the
    queue is dropped, and in-flight downloads finish and get reported. A second
    signal while downloads are still finishing forces an immediate exit.
    """
    import logging

    from . import deadline as window

    log = logging.getLogger("master_downloader_python")

    def handler(signum, _frame):
        name = signal.Signals(signum).name
        if window.expire_now(f"stopped by {name}"):
            log.info("%s received: window closed early, letting in-flight downloads finish "
                     "(send again to force quit)", name)
        else:
            log.warning("%s received while shutting down: forcing exit", name)
            os._exit(130)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# --------------------------------------------------------------------------
# parent process
# --------------------------------------------------------------------------
class LineCounter:
    """Counts lines appended to a file without re-reading it."""

    def __init__(self, path: Path):
        self.path, self.offset, self.count = path, 0, 0

    def poll(self) -> int:
        try:
            with open(self.path, "rb") as fh:
                fh.seek(self.offset)
                chunk = fh.read()
        except FileNotFoundError:
            return self.count
        self.offset += len(chunk)
        self.count += chunk.count(b"\n")
        return self.count


def main(argv: list[str] | None = None) -> None:
    cfg = load_config(argv)
    run_dir = Path(cfg.output_dir) / datetime.now().strftime("run-%Y%m%d-%H%M%S")
    ncpu = os.cpu_count() or 1

    ctx = mp.get_context("spawn")
    ready = ctx.Barrier(len(cfg.domains) + 1)
    go = ctx.Event()
    shared_deadline = ctx.Value("d", 0.0)
    procs: list[tuple[str, mp.Process, LineCounter]] = []
    for i, url in enumerate(cfg.domains):
        host = urlparse(url).netloc.replace(":", "_")  # host_port for local test sites
        out = run_dir / host
        out.mkdir(parents=True, exist_ok=True)
        settings_kwargs = dict(
            concurrency=cfg.concurrency_per_domain,
            download_timeout=cfg.download_timeout,
            obey_robots=cfg.obey_robots,
            out_dir=out,
        )
        cpu = (i % ncpu) if cfg.pin_cpu else None
        p = ctx.Process(
            target=run_domain,
            args=(url, str(out), settings_kwargs, cpu, cfg.include_subdomains, ready, go, shared_deadline),
            name=host,
        )
        p.start()
        procs.append((host, p, LineCounter(out / "pages.jsonl")))

    t_spawn = time.time()
    ready.wait(timeout=120)  # all processes imported Scrapy and are ready to go
    start = time.time()
    deadline = start + cfg.duration_seconds
    shared_deadline.value = deadline
    go.set()
    print(f"(startup took {start - t_spawn:.1f}s; not counted in the time window)")
    print(f"master-downloader-python: {len(procs)} processes on {ncpu} CPUs, window {cfg.duration_seconds:g}s, "
          f"{cfg.concurrency_per_domain} parallel requests per domain -> {run_dir}")
    # Ctrl-C reaches the children directly (same process group). `kill <pid>`
    # (SIGTERM) only reaches this parent, so it is forwarded to them.
    stopping = False

    def on_sigterm(_signum, _frame):
        nonlocal stopping
        if not stopping:
            print("SIGTERM: window closed early; letting in-flight downloads finish", flush=True)
        stopping = True
        for _, p, _ in procs:
            if p.is_alive():
                p.terminate()  # = SIGTERM -> graceful stop in the child

    signal.signal(signal.SIGTERM, on_sigterm)

    while any(p.is_alive() for _, p, _ in procs):
        try:
            # Print progress every 2 s, but notice the end immediately so the
            # measured wall time is exact.
            tick = time.time() + 2
            while time.time() < tick and any(p.is_alive() for _, p, _ in procs):
                time.sleep(0.05)
        except KeyboardInterrupt:
            if stopping:
                print("second Ctrl-C: forcing exit", flush=True)
            else:
                print("Ctrl-C: window closed early; letting in-flight downloads finish "
                      "(Ctrl-C again to force quit)", flush=True)
            stopping = True
            continue
        now = time.time()
        counts = [(h, c.poll()) for h, _, c in procs]
        total = sum(n for _, n in counts)
        phase = "draining in-flight" if (stopping or now >= deadline) else "crawling"
        print(f"[{now - start:6.1f}s {phase:>18}] total {total:>7} pages "
              f"({total / (now - start):7.1f}/s) | "
              + "  ".join(f"{h}: {n}" for h, n in counts), flush=True)
    for _, p, _ in procs:
        p.join()
    wall = time.time() - start

    write_report(run_dir, [h for h, _, _ in procs], cfg, wall, start, deadline)


def write_report(run_dir: Path, hosts: list[str], cfg: Config, wall: float,
                 start: float, deadline: float) -> None:
    rows: list[dict] = []
    per_domain: dict[str, dict] = {}
    started_late = finished_after = 0
    for host in hosts:
        dom_rows = []
        f = run_dir / host / "pages.jsonl"
        if f.exists():
            with open(f) as fh:
                dom_rows = [json.loads(line) for line in fh if line.strip()]
        rows.extend(dom_rows)
        stats_file = run_dir / host / "stats.json"
        st = json.loads(stats_file.read_text()) if stats_file.exists() else {}
        per_domain[host] = {
            "pages": len(dom_rows),
            "size_mb": round(sum(r["size_kb"] for r in dom_rows) / 1024, 3),
            "avg_speed_kb_s": round(statistics.fmean(r["speed_kb_s"] for r in dom_rows), 2) if dom_rows else 0,
            "errors": st.get("fastcrawl/errors", 0),
            "blocked_by_robots": st.get("fastcrawl/blocked_by_robots", 0),
            "skipped_non_html": st.get("fastcrawl/skipped_non_html", 0),
            # duplicate links skipped by the in-process pre-filter + Scrapy's dupefilter
            "duplicates_filtered": st.get("fastcrawl/duplicate_links_skipped", 0) + st.get("dupefilter/filtered", 0),
            "queued_but_not_started_at_deadline": st.get("fastcrawl/discarded_at_deadline", 0)
            + st.get("fastcrawl/not_started_deadline", 0),
            "close_reason": st.get("close_reason"),
            "window_close_reason": st.get("window_close_reason"),
        }
        d = per_domain[host]
        # The window closes at the deadline, or earlier on Ctrl-C / SIGTERM.
        closed_at = min(deadline, st.get("window_closed_at") or deadline)
        d["window_closed_after_s"] = round(closed_at - start, 3)
        started_late += sum(1 for r in dom_rows if (r.get("started_at") or 0) > closed_at)
        finished_after += sum(1 for r in dom_rows if r["finished_at"] > closed_at)
        # Nothing left to download before the window closed: the site is fully crawled.
        d["fully_crawled_before_deadline"] = bool(
            d["queued_but_not_started_at_deadline"] == 0
            and (not dom_rows or max(r["finished_at"] for r in dom_rows) < closed_at)
        )

    with open(run_dir / "report.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["domain", "url", "size_kb", "download_seconds", "speed_kb_s", "location",
                    "started_s", "finished_s"])  # seconds since the window opened
        for r in rows:
            w.writerow([r["domain"], r["url"], r["size_kb"], r["download_seconds"], r["speed_kb_s"], r["location"],
                        round((r.get("started_at") or start) - start, 3), round(r["finished_at"] - start, 3)])

    total_kb = sum(r["size_kb"] for r in rows)
    total_dl_time = sum(r["download_seconds"] for r in rows)
    unique_urls = len({r["url"] for r in rows})
    summary = {
        "window_seconds": cfg.duration_seconds,
        "wall_seconds": round(wall, 2),
        "pages": len(rows),
        "unique_urls": unique_urls,
        "total_size_mb": round(total_kb / 1024, 3),
        "pages_per_second": round(len(rows) / wall, 2) if wall else 0,
        # Mean of the per-page speeds (each page's size / its own transfer time).
        "average_download_speed_kb_s": round(statistics.fmean(r["speed_kb_s"] for r in rows), 2) if rows else 0,
        # Total bytes / total transfer time (weights pages by size).
        "size_weighted_download_speed_kb_s": round(total_kb / total_dl_time, 2) if total_dl_time else 0,
        # What the whole program achieved thanks to concurrency.
        "aggregate_throughput_kb_s": round(total_kb / wall, 2) if wall else 0,
        # Graceful stop check: must be 0 (no download starts after the window)...
        "downloads_started_after_deadline": started_late,
        # ...while pages already in flight are allowed to finish after it.
        "in_flight_pages_finished_after_deadline": finished_after,
        "domains": per_domain,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    # ---- console report ---------------------------------------------------
    print("\nPer-page report (first 15 rows; all rows in report.csv):")
    print(f"  {'size KB':>9} {'KB/s':>10}  url  ->  location")
    for r in rows[:15]:
        print(f"  {r['size_kb']:>9.1f} {r['speed_kb_s']:>10.1f}  {r['url']}  ->  {r['location']}")
    print("\nPer domain:")
    for h, d in per_domain.items():
        print(f"  {h:<32} pages={d['pages']:<7} MB={d['size_mb']:<9} avg={d['avg_speed_kb_s']} KB/s  "
              f"errors={d['errors']} non-html={d['skipped_non_html']} dupes={d['duplicates_filtered']}"
              + ("  [site fully crawled before the deadline]" if d["fully_crawled_before_deadline"] else ""))
    print(f"\nPages downloaded:            {summary['pages']}  (unique URLs: {unique_urls})")
    print(f"Total size:                  {summary['total_size_mb']} MB")
    print(f"Wall time:                   {summary['wall_seconds']} s  (window {cfg.duration_seconds:g} s)")
    print(f"Pages per second:            {summary['pages_per_second']}")
    print(f"Average download speed:      {summary['average_download_speed_kb_s']} KB/s per page")
    print(f"Size-weighted download speed:{summary['size_weighted_download_speed_kb_s']:>8} KB/s")
    print(f"Aggregate throughput:        {summary['aggregate_throughput_kb_s']} KB/s")
    reasons = sorted({d["window_close_reason"] for d in per_domain.values() if d["window_close_reason"]})
    print(f"Window closed:               {', '.join(reasons) or 'n/a'}")
    print(f"Graceful stop:               {started_late} downloads started after the window closed, "
          f"{finished_after} in-flight pages finished after it")
    print(f"\nReport:  {(run_dir / 'report.csv').resolve()}")
    print(f"Summary: {(run_dir / 'summary.json').resolve()}")


if __name__ == "__main__":
    main()
