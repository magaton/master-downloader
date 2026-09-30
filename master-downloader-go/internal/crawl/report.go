package crawl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"master-downloader-go/internal/config"
	"master-downloader-go/internal/window"
)

func writeReport(runDir string, domains []*domain, cfg config.Config, wall time.Duration, start, deadline time.Time, win *window.Window) error {
	rows := make([]pageRecord, 0)
	perDomain := map[string]map[string]any{}
	startedLate := 0
	finishedAfter := 0

	for _, d := range domains {
		domRows := readPages(filepath.Join(d.dir, "pages.jsonl"))
		rows = append(rows, domRows...)
		d.mu.Lock()
		discarded := d.discarded
		notStarted := d.notStarted
		d.mu.Unlock()
		queued := discarded + notStarted
		closedAt := deadline
		if at := win.At(); !at.IsZero() && at.Before(closedAt) {
			closedAt = at
		}
		var maxFinished float64
		for _, r := range domRows {
			if r.FinishedAt > maxFinished {
				maxFinished = r.FinishedAt
			}
			if r.StartedAt > unixFloat(closedAt) {
				startedLate++
			}
			if r.FinishedAt > unixFloat(closedAt) {
				finishedAfter++
			}
		}
		fully := queued == 0 && (len(domRows) == 0 || maxFinished < unixFloat(closedAt))
		perDomain[d.folder] = map[string]any{
			"pages":                              len(domRows),
			"size_mb":                            round(sumSize(domRows)/1024, 3),
			"avg_speed_kb_s":                     round(meanSpeed(domRows), 2),
			"errors":                             d.failures.Load(),
			"blocked_by_robots":                  d.blocked.Load(),
			"skipped_non_html":                   d.skipped.Load(),
			"duplicates_filtered":                d.dupes.Load(),
			"queued_but_not_started_at_deadline": queued,
			"close_reason":                       "finished",
			"window_close_reason":                win.Reason(),
			"window_closed_after_s":              round(closedAt.Sub(start).Seconds(), 3),
			"fully_crawled_before_deadline":      fully,
		}
	}

	csvPath := filepath.Join(runDir, "report.csv")
	if err := writeCSV(csvPath, rows, start); err != nil {
		return err
	}

	totalKB := sumSize(rows)
	totalDL := 0.0
	for _, r := range rows {
		totalDL += r.DownloadSeconds
	}
	unique := map[string]struct{}{}
	for _, r := range rows {
		unique[r.URL] = struct{}{}
	}
	wallSec := wall.Seconds()
	summary := map[string]any{
		"window_seconds":                          cfg.DurationSeconds,
		"wall_seconds":                            round(wallSec, 2),
		"pages":                                   len(rows),
		"unique_urls":                             len(unique),
		"total_size_mb":                           round(totalKB/1024, 3),
		"pages_per_second":                        round(safeDiv(float64(len(rows)), wallSec), 2),
		"average_download_speed_kb_s":             round(meanSpeed(rows), 2),
		"size_weighted_download_speed_kb_s":       round(safeDiv(totalKB, totalDL), 2),
		"aggregate_throughput_kb_s":               round(safeDiv(totalKB, wallSec), 2),
		"downloads_started_after_deadline":        startedLate,
		"in_flight_pages_finished_after_deadline": finishedAfter,
		"domains":                                 perDomain,
	}
	buf, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	summaryPath := filepath.Join(runDir, "summary.json")
	if err := os.WriteFile(summaryPath, append(buf, '\n'), 0o644); err != nil {
		return err
	}

	fmt.Println("\nPer-page report (first 15 rows; all rows in report.csv):")
	fmt.Printf("  %9s %10s  url  ->  location\n", "size KB", "KB/s")
	limit := 15
	if len(rows) < limit {
		limit = len(rows)
	}
	for _, r := range rows[:limit] {
		fmt.Printf("  %9.1f %10.1f  %s  ->  %s\n", r.SizeKB, r.SpeedKBs, r.URL, r.Location)
	}
	fmt.Println("\nPer domain:")
	for _, d := range domains {
		st := perDomain[d.folder]
		extra := ""
		if st["fully_crawled_before_deadline"] == true {
			extra = "  [site fully crawled before the deadline]"
		}
		fmt.Printf("  %-32s pages=%-7d MB=%-9v avg=%v KB/s  errors=%v non-html=%v dupes=%v%s\n",
			d.folder, st["pages"], st["size_mb"], st["avg_speed_kb_s"], st["errors"], st["skipped_non_html"], st["duplicates_filtered"], extra)
	}
	fmt.Printf("\nPages downloaded:            %d  (unique URLs: %d)\n", len(rows), len(unique))
	fmt.Printf("Total size:                  %v MB\n", summary["total_size_mb"])
	fmt.Printf("Wall time:                   %v s  (window %s s)\n", summary["wall_seconds"], trimFloat(cfg.DurationSeconds))
	fmt.Printf("Pages per second:            %v\n", summary["pages_per_second"])
	fmt.Printf("Average download speed:      %v KB/s per page\n", summary["average_download_speed_kb_s"])
	fmt.Printf("Size-weighted download speed:%8v KB/s\n", summary["size_weighted_download_speed_kb_s"])
	fmt.Printf("Aggregate throughput:        %v KB/s\n", summary["aggregate_throughput_kb_s"])
	fmt.Printf("Window closed:               %s\n", win.Reason())
	fmt.Printf("Graceful stop:               %d downloads started after the window closed, %d in-flight pages finished after it\n",
		startedLate, finishedAfter)
	absCSV, _ := filepath.Abs(csvPath)
	absSum, _ := filepath.Abs(summaryPath)
	fmt.Printf("\nReport:  %s\n", absCSV)
	fmt.Printf("Summary: %s\n", absSum)
	return nil
}

func readPages(path string) []pageRecord {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var rows []pageRecord
	for dec.More() {
		var rec pageRecord
		if err := dec.Decode(&rec); err != nil {
			break
		}
		rows = append(rows, rec)
	}
	return rows
}

func writeCSV(path string, rows []pageRecord, start time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"domain", "url", "size_kb", "download_seconds", "speed_kb_s", "location", "started_s", "finished_s"}); err != nil {
		return err
	}
	startUnix := unixFloat(start)
	for _, r := range rows {
		if err := w.Write([]string{
			r.Domain,
			r.URL,
			fmt.Sprintf("%v", r.SizeKB),
			fmt.Sprintf("%v", r.DownloadSeconds),
			fmt.Sprintf("%v", r.SpeedKBs),
			r.Location,
			fmt.Sprintf("%.3f", r.StartedAt-startUnix),
			fmt.Sprintf("%.3f", r.FinishedAt-startUnix),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func sumSize(rows []pageRecord) float64 {
	var n float64
	for _, r := range rows {
		n += r.SizeKB
	}
	return n
}

func meanSpeed(rows []pageRecord) float64 {
	if len(rows) == 0 {
		return 0
	}
	var n float64
	for _, r := range rows {
		n += r.SpeedKBs
	}
	return n / float64(len(rows))
}

func safeDiv(a, b float64) float64 {
	if b == 0 || math.IsNaN(b) {
		return 0
	}
	return a / b
}
