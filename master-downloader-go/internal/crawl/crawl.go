// Package crawl downloads HTML from the configured sites using Colly.
// Colly does the fetching, the per-domain concurrency limit, robots.txt,
// and the visited-URL set. This package adds the time window: after it
// closes, nothing new is sent, and requests already on the wire finish.
package crawl

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gocolly/colly/v2"

	"master-downloader-go/internal/config"
	"master-downloader-go/internal/window"
)

const userAgent = "Mozilla/5.0 (compatible; fastcrawl/0.1)"

// Extensions Colly must not request. Same idea as Scrapy's IGNORED_EXTENSIONS:
// images, video, and other non-HTML. Extensionless responses are aborted
// after the headers instead.
var ignoredExt = map[string]struct{}{
	"7z": {}, "7zip": {}, "bz2": {}, "rar": {}, "tar": {}, "xz": {}, "zip": {},
	"mng": {}, "pct": {}, "bmp": {}, "gif": {}, "jpg": {}, "jpeg": {}, "png": {},
	"pst": {}, "psp": {}, "tif": {}, "tiff": {}, "ai": {}, "drw": {}, "dxf": {},
	"eps": {}, "ps": {}, "svg": {}, "cdr": {}, "ico": {}, "webp": {},
	"mp3": {}, "wma": {}, "ogg": {}, "wav": {}, "ra": {}, "aac": {}, "mid": {}, "au": {}, "aiff": {},
	"3gp": {}, "asf": {}, "asx": {}, "avi": {}, "mov": {}, "mp4": {}, "mpg": {}, "qt": {},
	"rm": {}, "swf": {}, "wmv": {}, "m4a": {}, "m4v": {}, "flv": {}, "webm": {},
	"xls": {}, "xlsm": {}, "xlsx": {}, "xltm": {}, "xltx": {}, "potm": {}, "potx": {},
	"ppt": {}, "pptm": {}, "pptx": {}, "pps": {}, "doc": {}, "docb": {}, "docm": {},
	"docx": {}, "dotm": {}, "dotx": {}, "odt": {}, "ods": {}, "odg": {}, "odp": {},
	"css": {}, "pdf": {}, "exe": {}, "bin": {}, "rss": {}, "dmg": {}, "iso": {},
	"apk": {}, "jar": {}, "sh": {}, "rb": {}, "js": {}, "json": {}, "csv": {},
	"hta": {}, "bat": {}, "cpl": {}, "msi": {}, "msp": {}, "py": {},
}

type domain struct {
	startURL   string
	host       string // host[:port], used as the report domain
	folder     string
	dir        string
	pagesDir   string
	wwwHost    string
	baseHost   string // hostname without www
	subdomains bool

	col     *colly.Collector
	win     *window.Window
	log     *log.Logger
	logFile *os.File
	jsonl   *os.File
	jsonMu  sync.Mutex

	seen       sync.Map
	startTimes sync.Map

	mu         sync.Mutex
	waiting    int
	logged     bool
	discarded  int
	notStarted int

	pages    atomic.Int64
	dupes    atomic.Int64
	skipped  atomic.Int64
	blocked  atomic.Int64
	failures atomic.Int64
}

func Run(cfg config.Config) error {
	setupStart := time.Now()
	win := window.New()
	runDir := filepath.Join(cfg.OutputDir, time.Now().Format("run-20060102-150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}

	domains := make([]*domain, 0, len(cfg.Domains))
	for _, raw := range cfg.Domains {
		d, err := newDomain(raw, runDir, cfg, win)
		if err != nil {
			return err
		}
		domains = append(domains, d)
		win.OnExpire(d.snapshot)
	}
	defer func() {
		for _, d := range domains {
			d.close()
		}
	}()

	start := time.Now()
	deadline := start.Add(time.Duration(cfg.DurationSeconds * float64(time.Second)))
	win.Set(deadline)
	timer := time.AfterFunc(time.Until(deadline), func() {
		for _, d := range domains {
			d.snapshot()
		}
	})
	defer timer.Stop()

	fmt.Printf("(startup took %.1fs; not counted in the time window)\n", start.Sub(setupStart).Seconds())
	fmt.Printf("master-downloader-go: %d domains, GOMAXPROCS=%d, window %ss, %d parallel requests per domain -> %s\n",
		len(domains), runtime.GOMAXPROCS(0), trimFloat(cfg.DurationSeconds), cfg.ConcurrencyPerDomain, runDir)

	go watchSignals(win)

	var wg sync.WaitGroup
	for _, d := range domains {
		wg.Add(1)
		go func(d *domain) {
			defer wg.Done()
			d.run()
		}(d)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-done:
			break loop
		case <-ticker.C:
			printProgress(domains, start, win.Expired())
		}
	}
	printProgress(domains, start, win.Expired())
	wall := time.Since(start)
	return writeReport(runDir, domains, cfg, wall, start, deadline, win)
}

func newDomain(raw, runDir string, cfg config.Config, win *window.Window) (*domain, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	hostName := u.Hostname()
	base := hostName
	if strings.HasPrefix(base, "www.") {
		base = base[4:]
	}
	folder := strings.ReplaceAll(u.Host, ":", "_")
	dir := filepath.Join(runDir, folder)
	pages := filepath.Join(dir, "pages")
	for i := 0; i < 256; i++ {
		if err := os.MkdirAll(filepath.Join(pages, fmt.Sprintf("%02x", i)), 0o755); err != nil {
			return nil, err
		}
	}
	logFile, err := os.Create(filepath.Join(dir, "crawl.log"))
	if err != nil {
		return nil, err
	}
	jsonl, err := os.Create(filepath.Join(dir, "pages.jsonl"))
	if err != nil {
		logFile.Close()
		return nil, err
	}

	d := &domain{
		startURL:   raw,
		host:       u.Host,
		folder:     folder,
		dir:        dir,
		pagesDir:   pages,
		wwwHost:    withWWW(u.Host),
		baseHost:   base,
		subdomains: cfg.IncludeSubdomains,
		win:        win,
		log:        log.New(logFile, "", log.LstdFlags),
		logFile:    logFile,
		jsonl:      jsonl,
	}

	opts := []colly.CollectorOption{
		colly.Async(true),
		colly.UserAgent(userAgent),
		colly.MaxBodySize(10 * 1024 * 1024),
	}
	// Colly ignores robots.txt unless this is set back to false.
	c := colly.NewCollector(opts...)
	c.IgnoreRobotsTxt = !cfg.ObeyRobots
	c.DisableCookies()
	c.SetRequestTimeout(time.Duration(cfg.DownloadTimeout * float64(time.Second)))
	if err := c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: cfg.ConcurrencyPerDomain}); err != nil {
		return nil, err
	}
	c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("stopped after 5 redirects")
		}
		return nil
	})
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.ConcurrencyPerDomain * 2
	transport.MaxIdleConnsPerHost = cfg.ConcurrencyPerDomain
	transport.MaxConnsPerHost = cfg.ConcurrencyPerDomain
	transport.ForceAttemptHTTP2 = true
	c.WithTransport(transport)

	c.OnRequest(d.onRequest)
	c.OnRequestHeaders(d.onRequestHeaders)
	c.OnResponseHeaders(d.onResponseHeaders)
	c.OnResponse(d.onResponse)
	c.OnHTML("a[href], area[href]", d.onLink)
	c.OnError(d.onError)
	d.col = c
	return d, nil
}

func (d *domain) run() {
	raw := d.startURL
	if u, err := canonical(d.startURL); err == nil {
		raw = u.String()
	}
	d.mark(raw)
	if err := d.col.Visit(raw); err != nil {
		d.noteVisitErr(err, raw)
	}
	d.col.Wait()
}

func (d *domain) close() {
	if d.jsonl != nil {
		d.jsonl.Close()
	}
	d.writeStats()
	if d.logFile != nil {
		d.logFile.Close()
	}
}

func (d *domain) onRequest(r *colly.Request) {
	r.Headers.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")
	r.Headers.Set("Accept-Language", "en")
	if robots(r.URL) {
		return
	}
	d.mu.Lock()
	if d.win.Expired() {
		d.noteExpiredLocked()
		d.notStarted++
		d.mu.Unlock()
		r.Abort()
		return
	}
	d.waiting++
	d.mu.Unlock()
}

func (d *domain) onRequestHeaders(r *colly.Request) {
	if robots(r.URL) {
		return
	}
	d.mu.Lock()
	if d.waiting > 0 {
		d.waiting--
	}
	if d.win.Expired() {
		// The queue snapshot already counts requests that were waiting.
		// Only the one that first notices the deadline has left that counter.
		first := !d.logged
		d.noteExpiredLocked()
		if first {
			d.notStarted++
		}
		d.mu.Unlock()
		r.Abort()
		return
	}
	d.mu.Unlock()
	d.startTimes.Store(r.ID, time.Now())
}

func (d *domain) onResponseHeaders(r *colly.Response) {
	if r.Request == nil || r.Request.URL == nil || robots(r.Request.URL) {
		return
	}
	if r.StatusCode >= 300 && r.StatusCode < 400 {
		return
	}
	ct := strings.ToLower(strings.TrimSpace(r.Headers.Get("Content-Type")))
	ct, _, _ = strings.Cut(ct, ";")
	ct = strings.TrimSpace(ct)
	if ct != "" && ct != "text/html" && !strings.HasPrefix(ct, "application/xhtml") {
		r.Request.Abort()
	}
}

func (d *domain) onResponse(r *colly.Response) {
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return
	}
	ct := strings.ToLower(r.Headers.Get("Content-Type"))
	if ct != "" && !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		d.skipped.Add(1)
		return
	}
	pageURL := r.Request.URL.String()
	sum := sha1.Sum([]byte(pageURL))
	digest := hex.EncodeToString(sum[:])
	path := filepath.Join(d.pagesDir, digest[:2], digest+".html")
	if err := os.WriteFile(path, r.Body, 0o644); err != nil {
		d.failures.Add(1)
		d.log.Printf("write %s: %v", path, err)
		return
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	finished := time.Now()
	started := finished
	if v, ok := d.startTimes.Load(r.Request.ID); ok {
		started = v.(time.Time)
		d.startTimes.Delete(r.Request.ID)
	}
	seconds := finished.Sub(started).Seconds()
	if seconds <= 0 {
		seconds = 1e-6
	}
	sizeKB := float64(len(r.Body)) / 1024
	rec := pageRecord{
		Domain:          d.host,
		URL:             pageURL,
		SizeKB:          round(sizeKB, 3),
		DownloadSeconds: round(seconds, 6),
		SpeedKBs:        round(sizeKB/seconds, 3),
		Location:        abs,
		StartedAt:       unixFloat(started),
		FinishedAt:      unixFloat(finished),
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return
	}
	d.jsonMu.Lock()
	d.jsonl.Write(append(line, '\n'))
	d.jsonMu.Unlock()
	d.pages.Add(1)

	if d.win.Expired() {
		return
	}
}

func (d *domain) onLink(e *colly.HTMLElement) {
	if d.win.Expired() {
		return
	}
	href := strings.TrimSpace(e.Attr("href"))
	if href == "" || strings.HasPrefix(href, "#") {
		return
	}
	abs := e.Request.AbsoluteURL(href)
	u, err := canonical(abs)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	if !d.allows(u) || ignoredPath(u.Path) {
		return
	}
	s := u.String()
	if !d.mark(s) {
		d.dupes.Add(1)
		return
	}
	if d.win.Expired() {
		return
	}
	if err := d.col.Visit(s); err != nil {
		d.noteVisitErr(err, s)
	}
}

func (d *domain) onError(r *colly.Response, err error) {
	switch {
	case errors.Is(err, colly.ErrAbortedAfterHeaders):
		d.skipped.Add(1)
	case errors.Is(err, colly.ErrAbortedBeforeRequest):
		// Counted when the request was refused.
	case errors.Is(err, colly.ErrRobotsTxtBlocked):
		d.blocked.Add(1)
	default:
		var visited *colly.AlreadyVisitedError
		if errors.As(err, &visited) {
			d.dupes.Add(1)
			return
		}
		d.failures.Add(1)
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		url := ""
		if r != nil && r.Request != nil && r.Request.URL != nil {
			url = r.Request.URL.String()
		}
		d.log.Printf("Failed %s: %s", url, msg)
	}
}

func (d *domain) noteVisitErr(err error, raw string) {
	switch {
	case errors.Is(err, colly.ErrRobotsTxtBlocked):
		d.blocked.Add(1)
	case errors.Is(err, colly.ErrForbiddenURL), errors.Is(err, colly.ErrForbiddenDomain):
	default:
		var visited *colly.AlreadyVisitedError
		if errors.As(err, &visited) {
			d.dupes.Add(1)
			return
		}
		d.failures.Add(1)
		d.log.Printf("Failed %s: %s", raw, err.Error())
	}
}

func (d *domain) mark(raw string) bool {
	_, loaded := d.seen.LoadOrStore(raw, struct{}{})
	return !loaded
}

func (d *domain) allows(u *url.URL) bool {
	if d.subdomains {
		h := u.Hostname()
		if h != d.baseHost && !strings.HasSuffix(h, "."+d.baseHost) {
			return false
		}
		// A non-default port on the start URL stays that port, so the four
		// local test sites (same IP, different ports) do not mix.
		if portOf(d.host) != "" && portOf(u.Host) != portOf(d.host) {
			return false
		}
		return true
	}
	return u.Host == d.host || u.Host == d.wwwHost
}

func (d *domain) snapshot() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.noteExpiredLocked()
}

func (d *domain) noteExpiredLocked() {
	if d.logged {
		return
	}
	d.logged = true
	d.discarded = d.waiting
	reason := d.win.Reason()
	if reason == "" {
		reason = "time window closed"
	}
	msg := fmt.Sprintf("%s: no new downloads; %d queued requests discarded, waiting for in-flight downloads to finish",
		capitalize(reason), d.discarded)
	d.log.Print(msg)
	fmt.Println(msg)
}

func (d *domain) writeStats() {
	d.mu.Lock()
	discarded := d.discarded
	notStarted := d.notStarted
	d.mu.Unlock()
	stats := map[string]any{
		"fastcrawl/pages":                   d.pages.Load(),
		"fastcrawl/errors":                  d.failures.Load(),
		"fastcrawl/blocked_by_robots":       d.blocked.Load(),
		"fastcrawl/skipped_non_html":        d.skipped.Load(),
		"fastcrawl/duplicate_links_skipped": d.dupes.Load(),
		"fastcrawl/discarded_at_deadline":   discarded,
		"fastcrawl/not_started_deadline":    notStarted,
		"close_reason":                      "finished",
		"window_closed_at":                  unixFloat(d.win.At()),
		"window_close_reason":               d.win.Reason(),
	}
	buf, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(d.dir, "stats.json"), append(buf, '\n'), 0o644)
}

func canonical(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	u.Fragment = ""
	u.RawQuery = u.Query().Encode()
	return u, nil
}

func ignoredPath(p string) bool {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(p), "."))
	if ext == "" {
		return false
	}
	_, ok := ignoredExt[ext]
	return ok
}

func robots(u *url.URL) bool {
	return u != nil && (u.Path == "/robots.txt" || strings.HasSuffix(u.Path, "/robots.txt"))
}

func withWWW(host string) string {
	name, port, err := net.SplitHostPort(host)
	if err != nil {
		name = host
		port = ""
	}
	if strings.HasPrefix(name, "www.") {
		return host
	}
	name = "www." + name
	if port == "" {
		return name
	}
	return net.JoinHostPort(name, port)
}

func portOf(host string) string {
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		return ""
	}
	return port
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func unixFloat(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixNano()) / 1e9
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	if v >= 0 {
		return float64(int64(v*p+0.5)) / p
	}
	return float64(int64(v*p-0.5)) / p
}

func trimFloat(v float64) string {
	s := fmt.Sprintf("%g", v)
	return s
}

func printProgress(domains []*domain, start time.Time, draining bool) {
	var total int64
	parts := make([]string, 0, len(domains))
	for _, d := range domains {
		n := d.pages.Load()
		total += n
		parts = append(parts, fmt.Sprintf("%s: %d", d.folder, n))
	}
	elapsed := time.Since(start).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(total) / elapsed
	}
	phase := "crawling"
	if draining {
		phase = "draining in-flight"
	}
	fmt.Printf("[%6.1fs %18s] total %7d pages (%7.1f/s) | %s\n",
		elapsed, phase, total, rate, strings.Join(parts, "  "))
}

type pageRecord struct {
	Domain          string  `json:"domain"`
	URL             string  `json:"url"`
	SizeKB          float64 `json:"size_kb"`
	DownloadSeconds float64 `json:"download_seconds"`
	SpeedKBs        float64 `json:"speed_kb_s"`
	Location        string  `json:"location"`
	StartedAt       float64 `json:"started_at"`
	FinishedAt      float64 `json:"finished_at"`
}

func watchSignals(win *window.Window) {
	stop := make(chan os.Signal, 2)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	s := <-stop
	name := "SIGTERM"
	if s == syscall.SIGINT {
		name = "SIGINT"
	}
	if win.ExpireNow("stopped by " + name) {
		if name == "SIGINT" {
			fmt.Println("Ctrl-C: window closed early; letting in-flight downloads finish (Ctrl-C again to force quit)")
		} else {
			fmt.Printf("%s: window closed early; letting in-flight downloads finish\n", name)
		}
	}
	<-stop
	fmt.Println("second signal: forcing exit")
	os.Exit(130)
}
