# master-downloader

Download as many HTML pages as possible from 4 websites in a fixed time window.
When the window closes, no new download starts, and pages already in flight finish.

Two implementations, same config knobs, same four default sites, same local graceful-stop test:

| | stack | how to run |
|---|---|---|
| [master-downloader-go](master-downloader-go/README.md) | Go, [Colly](https://github.com/gocolly/colly) | `cd master-downloader-go && go run .` |
| [master-downloader-python](master-downloader-python/README.md) | Python, [Scrapy](https://scrapy.org) | `cd master-downloader-python && uv run master-downloader-python` |

## How the two implementations compare

The score is how many HTML pages finish inside the window. The machine has 4 cores, there are 4 domains, and every page is treated as the same size. Both programs meet the rest of the contract: HTML only, each URL once, a configured window, no new download after it closes, in-flight pages finish, and a report of URL, size in KB, file location, per-page speed, and the average.

They schedule the 4 cores differently, and that is what changes the page count.

The Python crawler runs one process per domain and, on Linux, pins that process to one core. That keeps all 4 cores busy while all 4 domains still have pages. When one domain runs out of pages, its core sits idle until the window ends, and the other domains cannot use it. Those are pages the window could still have downloaded.

The Go crawler is one process. `pin_cpu` sets `GOMAXPROCS` to 4, and any domain's work can run on any of those cores. Each domain still has its own cap (`concurrency_per_domain`); a finished domain does not hand that cap to the others. What it hands over is the core time. For the rest of the window, the domains that still have pages use all 4 cores.

The same-size assumption is why that schedule is enough. There is no large page to avoid and no small page to prefer, so the only waste to remove is a core with nothing left to download.

On the local test both finished the same run: 816 pages, 0 starts after the deadline, 256 pages still downloading when the window closed and then completed. Every page was delayed by 2 seconds, so both were waiting on the server and the core schedule never mattered. That run checks the graceful stop. The Go crawler pulls ahead when a domain runs out of pages before the window ends, or when pages return quickly enough that time spent parsing links shows up in the page count. Scrapy does that parsing under the GIL, which is why the Python crawler needed a process per core in the first place.

### Other languages

**Node** shares cores the way Go does while downloads are in flight: one event loop can talk to all 4 domains, so a finished domain does not leave a core idle. Link extraction still runs on the one JavaScript thread, so a slow parse stalls every domain at once. Worker threads or `cluster` get the other cores back by splitting the domains across processes, which is the Python schedule again. Crawlee is a real crawler library. The time window would still be custom, as it is in both implementations. Once pages return quickly enough that parsing fills the window, Go finishes more pages.

**Rust** matches Go on the 4-core schedule: one process, any domain on any core, each domain keeping its own request cap. It does less work per page, so it can finish somewhat more when sites answer quickly and link extraction fills the window. The graceful stop stays the same. On the 2-second local test it would tie with both. That edge over Go is small enough to keep the Go crawler. `tokio` with `reqwest`, or the `spider` crate, would be the library path.

**Java 21, Kotlin, and C#** beat the Python crawler on the same point as Go: all 4 domains in one process, so a finished domain leaves its core time for the domains that still have pages. They finish fewer pages than Go on a short window. The runtime takes longer to warm up, and that time comes out of the window. After warmup they still do more work per page than Go. `crawler4j` on the JVM, or `HttpClient` plus an HTML parser in C#, covers an existing crawler or HTTP library.

**Elixir and Ruby** stay with the Python crawler: many downloads at once, slower HTML parsing, and link extraction that does not use the other cores.
