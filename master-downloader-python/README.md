# master-downloader-python

A multi-domain HTML crawler built for throughput, based on [Scrapy](https://scrapy.org).
It downloads as many HTML pages as possible from 4 websites within a time window
you set, then reports every page and the average download speed.

## Run

Requires [uv](https://docs.astral.sh/uv/). uv installs Python 3.11 automatically if needed.

```bash
uv sync                      # create .venv and install dependencies (locked in uv.lock)
uv run master-downloader-python             # crawl with config.toml
uv build                     # optional: build sdist + wheel into dist/
```

Settings can be overridden on the command line:

```bash
uv run master-downloader-python -t 120                                   # 120-second window
uv run master-downloader-python -d site1.com site2.com site3.com site4.com
uv run master-downloader-python -c other.toml --concurrency 128 --no-robots
```

## Configuration (`config.toml`)

| key | meaning |
|---|---|
| `domains` | the 4 websites: start URLs or bare domains (`example.com` → `https://example.com/`) |
| `duration_seconds` | time window during which new downloads may start |
| `concurrency_per_domain` | parallel requests per site; the main throughput knob |
| `download_timeout` | abandon a request after this many seconds |
| `obey_robots` | respect robots.txt |
| `include_subdomains` | also crawl `*.example.com` (default: only `example.com` and `www.example.com`) |
| `pin_cpu` | pin each process to its own core (Linux) |
| `output_dir` | where pages and reports go |

## Output

```
output/run-YYYYmmdd-HHMMSS/
  report.csv          one row per page: domain, url, size_kb, download_seconds, speed_kb_s, location
  summary.json        totals, average speed, per-domain stats, graceful-stop check
  <domain>/pages/xx/<sha1(url)>.html   downloaded pages (256 shard dirs)
  <domain>/pages.jsonl                 raw per-page records
  <domain>/crawl.log, stats.json       Scrapy log and stats
```

The console shows live progress, the first per-page rows, per-domain totals, and:

* **Average download speed**: mean of the per-page speeds (page size ÷ that page's transfer time).
* **Size-weighted download speed**: total KB ÷ total transfer time.
* **Aggregate throughput**: total KB ÷ wall time. This is what concurrency buys you.
* **Graceful stop**: the number of downloads that started after the deadline (always 0) and
  the number of in-flight pages that were allowed to finish after it.

## How the requirements are met

| requirement | implementation |
|---|---|
| 4 CPU cores, 4 domains | One OS process per domain (`multiprocessing`, spawn), each pinned to a core. Scrapy runs on one core per process, so this uses all 4 without the GIL getting in the way. |
| Time window set by the user | All processes do their slow imports first and wait at a barrier. The clock starts when all are ready, so every domain gets the same window. |
| No new downloads after time is up; in-flight downloads finish | `DeadlineScheduler` stops handing out requests and reports "nothing pending". `TimedHTTP11DownloadHandler` refuses anything that reaches the wire after the deadline. In-flight transfers are never cancelled, and the spider closes when they are done. Ctrl-C / SIGTERM close the window early through the same path. |
| HTML only, no images or video | `LinkExtractor` never follows media/binary extensions. `Accept: text/html`. Responses whose `Content-Type` is not HTML are aborted right after the headers arrive (`StopDownload`), so no body bytes are wasted. Pages' sub-resources (img, css, js) are never fetched. |
| Each unique page only once | Scrapy fingerprint dupefilter (canonical URL: sorted query, no `#fragment`), which also covers redirect targets, plus a cheap in-process `seen` set that skips known links before a Request is even created. |
| URL, size (KB), location, speed per page + average | `report.csv` / `summary.json`. Speed is timed inside the download handler: full body, no queueing or parsing time. |

## Throughput optimisations

1. **Process per domain × core** – real parallelism for parsing/link extraction; 4 independent event loops.
2. **High async concurrency** – 64 parallel requests per domain by default, no download delay, AutoThrottle off.
   `CONCURRENT_REQUESTS == CONCURRENT_REQUESTS_PER_DOMAIN`, so requests never wait in a per-domain queue.
3. **Connection reuse** – HTTP/1.1 keep-alive pool sized to the concurrency; DNS cache.
4. **Compression** – gzip/deflate responses (fewer bytes on the wire).
5. **Early abort of non-HTML** – after the headers, before the body.
6. **Fail fast** – short timeout, 1 retry, capped redirects and max size, so slow pages don't hold connection slots.
7. **Breadth-first frontier** – keeps the queue wide, so all connection slots stay busy.
8. **Cheap duplicate pre-filter** – most links on a page (menus, footers) are skipped with a set lookup.
9. **Non-blocking disk I/O** – page bodies are written by a thread pool; the event loop only does network work.
10. **Less per-request work** – cookies, referer, HTTP auth, telnet, memusage and cache middlewares are off;
    uvloop drives the asyncio reactor; link extraction stops once the window closes.
11. **Fair, exact time window** – startup cost is excluded, and the wall time is measured precisely.

## Testing the requirements locally

`tools/test_server.py` starts 4 local test sites. Each is an endless tree of pages of the same
size, every page is slow on purpose (`--delay`), and every page includes traps: duplicate links,
images, videos, a PDF behind a link with no file extension, and a robots-forbidden page. The server
counts what it serves, so it checks the crawler's report independently.

```bash
# terminal 1
uv run python tools/test_server.py --delay 2      # 4 sites on 127.0.0.1:8001-8004
# terminal 2
uv run master-downloader-python -c config.local.toml -t 10
```

**Graceful stop when the time is up.** With 64 parallel requests per site and 2 s per page, about
256 downloads are in flight when the window closes. Expected result:

* the progress line switches to `draining in-flight` and keeps counting for about `--delay` seconds;
* `crawl.log`: `Time window closed: no new downloads; N queued requests discarded, waiting for in-flight downloads to finish`;
* summary: `Graceful stop: 0 downloads started after the window closed, ~256 in-flight pages finished after it`;
* `report.csv` has `started_s` / `finished_s` columns: every page started before the window
  closed, and the in-flight ones finished after it.

**Graceful stop on demand.** Start a long run (`-t 300`) and press **Ctrl-C** once, or run
`kill <pid>` (SIGTERM). The window closes immediately and takes the same graceful path; the summary
shows `Window closed: stopped by SIGINT` (or `SIGTERM`). Press Ctrl-C a second time to force quit.

**The other requirements.** In the test server's final line, after Ctrl-C:

| server counter | expected | proves |
|---|---|---|
| `HTML pages served` | = crawler's `Pages downloaded` | every page counted and reported |
| `duplicate page requests` | 0 | each unique page downloaded once |
| `media requests` | 0 | images and videos ignored |
| `private requests` | 0 | robots.txt respected |
| `PDF downloads aborted` | = `PDF requests` | non-HTML aborted after the headers, body not downloaded |

Use `--delay 0` to measure raw throughput. The test server is plain Python, so on a
4-core machine it may itself become the bottleneck.

## Tuning and caveats

* The default sites (`toscrape.com`, `web-scraping.dev`, `webscraper.io`) are built for scraper practice, but they are
  small: all of them are fully crawled in about 20 s. The report marks such domains
  `[site fully crawled before the deadline]`, and the program ends early. For a real benchmark, use
  larger sites you are allowed to crawl.
* Throughput is usually limited by the target servers or by your bandwidth, not by the CPU. Raise
  `concurrency_per_domain` until pages/s stops improving or errors (timeouts, 429) appear. Be considerate:
  high concurrency against a site you don't own can look like a denial-of-service attack.
* If one site runs out of pages early, its core sits idle (by design: one process per domain).
* Duplicate detection is URL-based. The same page under two different URLs (e.g. `/` and `/index.html`) counts twice.

## Where this sits against other implementations

The score is how many HTML pages finish inside the window. The machine has 4 cores, there are 4 domains, and every page is treated as the same size. This program, the [Go crawler](../master-downloader-go/README.md), and the alternatives below can all do the rest: HTML only, each URL once, a configured window, no new download after it closes, in-flight pages finish, and the per-page report plus the average. See the [repo README](../README.md) for how to run both.

**Go is the better fit.** This crawler runs one process per domain and, on Linux, pins that process to one core. All 4 cores stay busy while all 4 domains still have pages. When one domain runs out of pages, its core sits idle until the window ends, and the other domains cannot use it. The Go crawler is one process with `GOMAXPROCS` at 4, so any domain can run on any of those cores. Each domain keeps its own request cap. A finished domain does not hand that cap over. It hands over the core time, and the domains that still have pages use all 4 cores for the rest of the window. The same-size assumption is why that is enough: there is no large page to avoid and no small page to prefer.

On the local test both saved 816 pages, started nothing after the deadline, and finished 256 pages that were already downloading. Every page was delayed by 2 seconds, so both were waiting on the server. That run checks the graceful stop. It does not rank them on page count. Go pulls ahead when a domain runs out of pages before the window ends, or when pages return quickly enough that Scrapy's link parsing under the GIL shows up in the page count. That parsing is why this program needed a process per core.

**Node would not beat Go, and it is not a clear step up from this program.** One event loop can download from all 4 domains without pinning a core to each. Link extraction still runs on the one JavaScript thread, so a slow parse stalls every domain at once. Worker threads or `cluster` get the other cores back by splitting the domains across processes again, which brings the idle core back. Crawlee is a real crawler library. The time window would still be custom, as it is here and in the Go crawler.

**Rust is not better than Go for this requirement.** It uses all 4 cores in one process, and it does less work per page, so it can save somewhat more when sites answer quickly and link extraction fills the window. That does not change the graceful stop or the 4-core schedule. On the 2-second local test it would tie. The gap is not large enough to replace the Go crawler. `tokio` with `reqwest`, or the `spider` crate, would be the library path.

**Java 21, Kotlin, and C# beat this program and lose to Go.** They also run all 4 domains in one process, so a finished domain does not leave a core idle. `crawler4j` on the JVM, or `HttpClient` plus an HTML parser in C#, covers the existing-library requirement. Their runtimes take longer to warm up, and that time comes out of a short window. After warmup they still do more work per page than Go.

Elixir and Ruby stay in this program's class: many downloads at once, slower HTML parsing, and no extra cores working on links.
