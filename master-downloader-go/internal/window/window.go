// Package window is the moment after which no new download may start.
// Ctrl-C or SIGTERM moves it to now, so an early stop takes the same path
// as the end of the time window.
package window

import (
	"sync"
	"time"
)

type Window struct {
	mu     sync.Mutex
	at     time.Time
	reason string
	hooks  []func()
}

func New() *Window {
	return &Window{reason: "time window closed"}
}

func (w *Window) Set(at time.Time) {
	w.mu.Lock()
	w.at = at
	w.reason = "time window closed"
	w.mu.Unlock()
}

func (w *Window) At() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.at
}

func (w *Window) Reason() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reason
}

func (w *Window) Expired() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.at.IsZero() && !time.Now().Before(w.at)
}

// OnExpire registers a function called once, when the window first closes.
func (w *Window) OnExpire(fn func()) {
	w.mu.Lock()
	w.hooks = append(w.hooks, fn)
	w.mu.Unlock()
}

// ExpireNow closes the window. It returns false if the window was already closed.
func (w *Window) ExpireNow(why string) bool {
	w.mu.Lock()
	if !w.at.IsZero() && !time.Now().Before(w.at) {
		w.mu.Unlock()
		return false
	}
	w.at = time.Now()
	w.reason = why
	hooks := append([]func(){}, w.hooks...)
	w.mu.Unlock()
	for _, fn := range hooks {
		fn()
	}
	return true
}
