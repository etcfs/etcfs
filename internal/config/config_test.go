package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The constraint the daemon's timing rests on: a request must be able to fail
// on its own deadline before the self-fencing watchdog takes the process down
// under it. A 3s lease TTL inverts the two, and used to be accepted.
func TestSelfFenceWindowClearsTheRequestTimeout(t *testing.T) {
	for _, ttl := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
		if SelfFenceWindow(ttl) > RequestTimeout {
			t.Errorf("lease TTL %s should be rejected: window %s exceeds the %s request timeout",
				ttl, SelfFenceWindow(ttl), RequestTimeout)
		}
	}
	for _, ttl := range []time.Duration{6 * time.Second, 10 * time.Second, time.Minute} {
		if SelfFenceWindow(ttl) <= RequestTimeout {
			t.Errorf("lease TTL %s should be accepted: window %s is under the %s request timeout",
				ttl, SelfFenceWindow(ttl), RequestTimeout)
		}
	}
}

// newTestFlags mirrors the shape Parse builds — a string, a bool, an int and a
// duration-carrying string — without touching the global command line.
func newTestFlags() (*flag.FlagSet, *string, *bool, *int) {
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	endpoints := set.String("etcd-endpoints", "http://localhost:2379", "")
	pageCache := set.Bool("page-cache", true, "")
	logLevel := set.Int("log-level", 1, "")
	set.Bool("fsck", false, "")
	return set, endpoints, pageCache, logLevel
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "etcfuse-meta.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The whole point of the layering: a flag beats an environment variable beats
// the file, and anything none of them mentions keeps its default.
func TestPrecedenceIsFlagThenEnvThenFile(t *testing.T) {
	set, endpoints, pageCache, logLevel := newTestFlags()
	if err := set.Parse([]string{"-etcd-endpoints=http://flag:2379"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETCFS_PAGE_CACHE", "false")

	file := map[string]any{
		"etcd-endpoints": "http://file:2379",
		"page-cache":     true,
		"log-level":      2,
	}
	if err := applyLayers(set, file); err != nil {
		t.Fatal(err)
	}

	if *endpoints != "http://flag:2379" {
		t.Errorf("endpoints = %q, want the command line's value", *endpoints)
	}
	if *pageCache {
		t.Error("page-cache should come from the environment, not the file")
	}
	if *logLevel != 2 {
		t.Errorf("log-level = %d, want 2 from the file", *logLevel)
	}
}

// A YAML list is the natural way to write endpoints, and the flag already
// parses the comma-separated form.
func TestFileListBecomesCommaSeparated(t *testing.T) {
	set, endpoints, _, _ := newTestFlags()
	if err := set.Parse(nil); err != nil {
		t.Fatal(err)
	}
	file := map[string]any{"etcd-endpoints": []any{"http://a:2379", "http://b:2379"}}
	if err := applyLayers(set, file); err != nil {
		t.Fatal(err)
	}
	if want := "http://a:2379,http://b:2379"; *endpoints != want {
		t.Errorf("endpoints = %q, want %q", *endpoints, want)
	}
}

// A typo in a config file must not read as a setting that silently does
// nothing, and a mode flag must not be reachable from one at all.
func TestFileRejectsUnknownAndModeKeys(t *testing.T) {
	for name, file := range map[string]map[string]any{
		"unknown": {"page-cahce": true},
		"mode":    {"fsck": true},
	} {
		set, _, _, _ := newTestFlags()
		if err := set.Parse(nil); err != nil {
			t.Fatal(err)
		}
		if err := applyLayers(set, file); err == nil {
			t.Errorf("%s key: want an error, got none", name)
		}
	}
}

// The default file is optional; one asked for by name is not.
func TestMissingFileIsAnErrorOnlyWhenNamed(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	if _, err := loadFile(missing, false); err != nil {
		t.Errorf("absent default file: %v", err)
	}
	if _, err := loadFile(missing, true); err == nil {
		t.Error("absent named file: want an error, got none")
	}

	path := writeConfig(t, "log-level: 2\n")
	file, err := loadFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if file["log-level"] != 2 {
		t.Errorf("log-level = %v, want 2", file["log-level"])
	}
}
