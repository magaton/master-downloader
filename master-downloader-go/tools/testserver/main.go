// Local test websites for the graceful-stop check.
//
// Four sites on 127.0.0.1:8001-8004. Each page is the same size, takes
// --delay seconds to download, and links to more pages plus traps:
// duplicates, images, video, a PDF with no file extension, and a page
// disallowed by robots.txt.
//
//	go run ./tools/testserver --delay 2
//	go run . -c config.local.toml -t 10
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "

type counters struct {
	mu    sync.Mutex
	stats map[string]int
	seen  map[int]map[string]struct{}
}

func (c *counters) add(key string, n int) {
	c.mu.Lock()
	c.stats[key] += n
	c.mu.Unlock()
}

func (c *counters) notePage(port int, path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	site := c.seen[port]
	if site == nil {
		site = map[string]struct{}{}
		c.seen[port] = site
	}
	if _, ok := site[path]; ok {
		c.stats["duplicate page requests"]++
	}
	site[path] = struct{}{}
	c.stats["HTML pages served"]++
}

func (c *counters) line() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := []string{
		"HTML pages served",
		"duplicate page requests",
		"media requests",
		"private requests",
		"PDF requests",
		"PDF downloads aborted",
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s: %d", k, c.stats[k])
	}
	return "  " + strings.Join(parts, " | ")
}

func main() {
	portsFlag := flag.String("ports", "8001,8002,8003,8004", "comma-separated ports")
	delay := flag.Float64("delay", 2, "seconds each page takes to download")
	sizeKB := flag.Int("size", 30, "page size in KB")
	flag.Parse()

	var ports []int
	for _, p := range strings.Split(*portsFlag, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad port:", p)
			os.Exit(1)
		}
		ports = append(ports, n)
	}

	ct := &counters{stats: map[string]int{}, seen: map[int]map[string]struct{}{}}
	padding := strings.Repeat(lorem, (*sizeKB)*1024/len(lorem)+1)

	for _, port := range ports {
		port := port
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			handle(w, r, port, *sizeKB, *delay, padding, ct)
		})
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		go srv.Serve(ln)
	}

	addrs := make([]string, len(ports))
	for i, p := range ports {
		addrs[i] = fmt.Sprintf("http://127.0.0.1:%d/", p)
	}
	fmt.Printf("Serving %d test sites: %s  (page size %d KB, %gs per page). Ctrl-C to stop.\n",
		len(ports), strings.Join(addrs, ", "), *sizeKB, *delay)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			fmt.Println(ct.line())
		case <-sig:
			fmt.Println("\nFinal:")
			fmt.Println(ct.line())
			return
		}
	}
}

func handle(w http.ResponseWriter, r *http.Request, port, sizeKB int, delay float64, padding string, ct *counters) {
	path := r.URL.Path
	if i := strings.IndexByte(path, '#'); i >= 0 {
		path = path[:i]
	}
	switch {
	case path == "/robots.txt":
		send(w, 200, "text/plain", []byte("User-agent: *\nDisallow: /private/\n"), delay, 1, ct, false)
		return
	case strings.HasPrefix(path, "/img/") || strings.HasPrefix(path, "/video/"):
		ct.add("media requests", 1)
		body := append([]byte{0xff, 0xd8}, []byte(strings.Repeat("0", 50000))...)
		send(w, 200, "image/jpeg", body, delay, 10, ct, false)
		return
	case strings.HasPrefix(path, "/private/"):
		ct.add("private requests", 1)
		send(w, 200, "text/html", []byte("<html>private</html>"), delay, 10, ct, false)
		return
	case strings.HasPrefix(path, "/download/"):
		ct.add("PDF requests", 1)
		body := append([]byte("%PDF-1.4"), []byte(strings.Repeat("0", 1_000_000))...)
		send(w, 200, "application/pdf", body, delay, 100, ct, true)
		return
	}

	page := 0
	if path != "/" {
		if !strings.HasPrefix(path, "/p/") || !digits(path[3:]) {
			send(w, 404, "text/html", []byte("<html>not found</html>"), delay, 1, ct, false)
			return
		}
		page, _ = strconv.Atoi(path[3:])
	}
	ct.notePage(port, path)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("<!doctype html><html><head><title>Site %d page %d</title></head><body>", port, page))
	b.WriteString(fmt.Sprintf("<h1>Site %d / page %d</h1>", port, page))
	parent := 0
	if page > 0 {
		parent = (page - 1) / 10
	}
	b.WriteString(fmt.Sprintf(`<nav><a href="/">home</a> <a href="/p/%d">parent</a> <a href="/p/%d#top">this page again</a></nav><ul>`, parent, page))
	for i := 1; i <= 10; i++ {
		n := page*10 + i
		fmt.Fprintf(&b, `<li><a href="/p/%d">page %d</a></li>`, n, n)
	}
	fmt.Fprintf(&b, `</ul><img src="/img/%d.jpg"><a href="/img/%d.jpg">photo</a> <a href="/video/%d.mp4">video</a> <a href="/download/%d">report (PDF)</a> <a href="/private/%d">private</a><p>`,
		page, page, page, page, page)
	html := []byte(b.String())
	tail := []byte("</p></body></html>")
	need := sizeKB*1024 - len(html) - len(tail)
	if need < 0 {
		need = 0
	}
	body := make([]byte, 0, len(html)+need+len(tail))
	body = append(body, html...)
	pad := []byte(padding)
	if need > len(pad) {
		need = len(pad)
	}
	body = append(body, pad[:need]...)
	body = append(body, tail...)
	send(w, 200, "text/html; charset=utf-8", body, delay, 10, ct, false)
}

func send(w http.ResponseWriter, status int, ctype string, body []byte, delay float64, slices int, ct *counters, pdf bool) {
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	flusher, _ := w.(http.Flusher)
	if delay <= 0 || slices == 1 || len(body) == 0 {
		if _, err := w.Write(body); err != nil {
			noteAbort(ct, pdf)
		}
		return
	}
	step := len(body) / slices
	if step < 1 {
		step = 1
	}
	pause := time.Duration(delay / float64(slices) * float64(time.Second))
	for i := 0; i < len(body); i += step {
		time.Sleep(pause)
		end := i + step
		if end > len(body) {
			end = len(body)
		}
		if _, err := w.Write(body[i:end]); err != nil {
			noteAbort(ct, pdf)
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func noteAbort(ct *counters, pdf bool) {
	ct.add("transfers aborted by client", 1)
	if pdf {
		ct.add("PDF downloads aborted", 1)
	}
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
