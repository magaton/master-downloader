# master-downloader-go

A multi-domain HTML crawler built for throughput, based on [Colly](https://github.com/gocolly/colly).
It downloads as many HTML pages as possible from 4 websites within a time window
you set, then reports every page and the average download speed.

Colly fetches pages, deduplicates URLs, applies robots.txt, and caps how many
requests are in flight per domain. The time window is ours: when it closes, Colly
is told to abort anything that has not yet gone on the wire, and downloads already
in flight run to completion.

Go is a single process. `pin_cpu` sets `GOMAXPROCS` to one thread per domain,
and any domain can run on any of those threads. Each domain keeps its own
request cap. When one domain runs out of pages, the cores it was using stay
available to the domains that still have pages.

## Run

```bash
go run .                          # crawl with config.toml
go run . -t 120                   # 120-second window
go run . -d site1.com site2.com site3.com site4.com
go run . -c other.toml --concurrency 128 --no-robots
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
| `pin_cpu` | cap `GOMAXPROCS` at one thread per domain |
| `output_dir` | where pages and reports go |

The default sites are the same four as the Python crawler: `books.toscrape.com`,
`quotes.toscrape.com`, `web-scraping.dev`, and `webscraper.io/test-sites`.

## Output

```
output/run-YYYYmmdd-HHMMSS/
  report.csv          one row per page: domain, url, size_kb, download_seconds, speed_kb_s, location, started_s, finished_s
  summary.json        totals, average speed, per-domain stats, graceful-stop check
  <domain>/pages/xx/<sha1(url)>.html
  <domain>/pages.jsonl
  <domain>/crawl.log, stats.json
```

* **Average download speed**: mean of the per-page speeds (page size ÷ that page's transfer time).
* **Size-weighted download speed**: total KB ÷ total transfer time.
* **Aggregate throughput**: total KB ÷ wall time.
* **Graceful stop**: downloads that started after the deadline (always 0), and in-flight pages that finished after it.

## Graceful shutdown test

`tools/testserver` serves the same four local sites as the Python test server.
Each site is an endless tree of equal-sized pages. Every page is slow (`--delay`)
and contains duplicate links, an image, a video, a PDF with no file extension,
and a robots-forbidden URL. The server counts what it actually serves.

```bash
# terminal 1
go run ./tools/testserver --delay 2
# terminal 2
go run . -c config.local.toml -t 10
```

With 64 requests per site and 2s per page, a full wave is still in flight when
the window closes. Expected:

* progress switches to `draining in-flight` and keeps counting for about `--delay` seconds
* `crawl.log`: `Time window closed: no new downloads; N queued requests discarded, waiting for in-flight downloads to finish`
* summary: `Graceful stop: 0 downloads started after the window closed`, and the in-flight pages finished after it
* `report.csv` `started_s` / `finished_s`: every page started before the window closed

Ctrl-C or SIGTERM closes the window immediately and uses that same path.
A second signal exits at once. The summary then shows `Window closed: stopped by SIGINT` (or `SIGTERM`).

After Ctrl-C on the test server, its counters should read:

| server counter | expected |
|---|---|
| `HTML pages served` | crawler's `Pages downloaded` |
| `duplicate page requests` | 0 |
| `media requests` | 0 |
| `private requests` | 0 |
| `PDF downloads aborted` | `PDF requests` |
