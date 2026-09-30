package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"master-downloader-go/internal/config"
	"master-downloader-go/internal/crawl"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	domains, rest := splitDomains(args)
	fs := flag.NewFlagSet("master-downloader-go", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("c", "config.toml", "TOML config file")
	fs.StringVar(configPath, "config", "config.toml", "TOML config file")
	duration := fs.Float64("t", -1, "override duration_seconds")
	fs.Float64Var(duration, "duration", -1, "override duration_seconds")
	concurrency := fs.Int("concurrency", -1, "override concurrency_per_domain")
	output := fs.String("output", "", "override output_dir")
	noRobots := fs.Bool("no-robots", false, "ignore robots.txt")
	if err := fs.Parse(rest); err != nil {
		return err
	}

	required := *configPath != "config.toml" || len(domains) == 0
	if _, err := os.Stat(*configPath); err != nil && *configPath == "config.toml" && len(domains) > 0 {
		required = false
	}
	cfg, err := config.LoadOptional(*configPath, required)
	if err != nil {
		return err
	}
	if len(domains) > 0 {
		cfg.Domains = domains
	}
	if *duration >= 0 {
		cfg.DurationSeconds = *duration
	}
	if *concurrency > 0 {
		cfg.ConcurrencyPerDomain = *concurrency
	}
	if *output != "" {
		cfg.OutputDir = *output
	}
	if *noRobots {
		cfg.ObeyRobots = false
	}
	if err := cfg.NormaliseDomains(); err != nil {
		return err
	}
	if len(cfg.Domains) != 4 {
		fmt.Fprintf(os.Stderr, "note: %d domains configured (the task expects 4); running them all\n", len(cfg.Domains))
	}
	if cfg.PinCPU {
		n := runtime.NumCPU()
		if len(cfg.Domains) < n {
			n = len(cfg.Domains)
		}
		if n < 1 {
			n = 1
		}
		runtime.GOMAXPROCS(n)
	}
	return crawl.Run(cfg)
}

// splitDomains pulls `-d a b c` / `--domains a b c` out before flag parsing,
// so the command line matches the Python crawler.
func splitDomains(args []string) (domains []string, rest []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		name := a
		if strings.HasPrefix(a, "--domains=") || strings.HasPrefix(a, "-d=") {
			raw := a[strings.IndexByte(a, '=')+1:]
			domains = append(domains, splitList(raw)...)
			continue
		}
		if name == "-d" || name == "--domains" {
			for i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
				domains = append(domains, splitList(args[i])...)
			}
			continue
		}
		rest = append(rest, a)
	}
	return domains, rest
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
