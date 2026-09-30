package config

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Domains              []string `toml:"domains"`
	DurationSeconds      float64  `toml:"duration_seconds"`
	OutputDir            string   `toml:"output_dir"`
	ConcurrencyPerDomain int      `toml:"concurrency_per_domain"`
	DownloadTimeout      float64  `toml:"download_timeout"`
	ObeyRobots           bool     `toml:"obey_robots"`
	IncludeSubdomains    bool     `toml:"include_subdomains"`
	PinCPU               bool     `toml:"pin_cpu"`
}

func Defaults() Config {
	return Config{
		DurationSeconds:      60,
		OutputDir:            "output",
		ConcurrencyPerDomain: 64,
		DownloadTimeout:      15,
		ObeyRobots:           true,
		IncludeSubdomains:    false,
		PinCPU:               true,
	}
}

func Load(path string) (Config, error) {
	cfg := Defaults()
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	dec := toml.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// LoadOptional reads path when it exists. A missing default config.toml is
// allowed when domains are passed on the command line.
func LoadOptional(path string, required bool) (Config, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return Defaults(), nil
		}
		return Config{}, fmt.Errorf("config file not found: %s", path)
	}
	return Load(path)
}

func Normalise(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty domain")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("invalid domain/URL: %q", raw)
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

func (c *Config) NormaliseDomains() error {
	out := make([]string, 0, len(c.Domains))
	seen := map[string]struct{}{}
	for _, d := range c.Domains {
		u, err := Normalise(d)
		if err != nil {
			return err
		}
		parsed, _ := url.Parse(u)
		if _, ok := seen[parsed.Host]; ok {
			return fmt.Errorf("each domain must be different")
		}
		seen[parsed.Host] = struct{}{}
		out = append(out, u)
	}
	if len(out) == 0 {
		return fmt.Errorf("no domains configured")
	}
	c.Domains = out
	return nil
}

func DecodeReader(r io.Reader) (Config, error) {
	cfg := Defaults()
	dec := toml.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
