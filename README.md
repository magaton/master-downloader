# master-downloader

Download as many HTML pages as possible from 4 websites in a fixed time window.
When the window closes, no new download starts, and pages already in flight finish.

Two implementations, same config knobs, same four default sites, same local graceful-stop test:

| | stack | how to run |
|---|---|---|
| [master-downloader-go](master-downloader-go/README.md) | Go, [Colly](https://github.com/gocolly/colly) | `cd master-downloader-go && go run .` |
| [master-downloader-python](master-downloader-python/README.md) | Python, [Scrapy](https://scrapy.org) | `cd master-downloader-python && uv run master-downloader-python` |

## Why the Go implementation is the better fit

The requirement is the number of HTML pages finished inside the window. The machine has 4 cores, there are 4 domains, and every page is treated as the same size. Both programs meet the rest of the contract: HTML only, each URL once, a configured window, no new download after it closes, in-flight pages finish, and a report of URL, size in KB, file location, per-page speed, and the average.

They schedule the 4 cores differently, and that is what changes the page count.

The Python crawler runs one process per domain and, on Linux, pins that process to one core. That keeps all 4 cores busy while all 4 domains still have pages. When one domain runs out of pages, its core sits idle until the window ends, and the other domains cannot use it. Those are pages the window could still have downloaded.

The Go crawler is one process. `pin_cpu` sets `GOMAXPROCS` to 4, and any domain's work can run on any of those cores. Each domain still has its own cap (`concurrency_per_domain`); a finished domain does not hand that cap to the others. What it hands over is the core time. For the rest of the window, the domains that still have pages use all 4 cores.

The same-size assumption is why that schedule is enough. There is no large page to avoid and no small page to prefer, so the only waste to remove is a core with nothing left to download.

On the local test both finished the same run: 816 pages, 0 starts after the deadline, 256 pages still downloading when the window closed and then completed. Every page was delayed by 2 seconds, so both were waiting on the server and the core schedule never mattered. That run checks the graceful stop. It does not rank them on page count. The Go crawler pulls ahead when a domain runs out of pages before the window ends, or when pages return quickly enough that time spent parsing links shows up in the page count. Scrapy does that parsing under the GIL, which is why the Python crawler needed a process per core in the first place.
