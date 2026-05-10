package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOptionsFromYAML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	payload := []byte(`
app:
  name: resilix-test
redis:
  addrs: ["127.0.0.1:6380"]
runtime:
  max_inflight: 32
`)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	options, err := LoadOptions(path, "RESILIX", map[string]any{"redis.password": "secret"})
	if err != nil {
		t.Fatalf("load options: %v", err)
	}

	if options.Name != "resilix-test" {
		t.Fatalf("unexpected name: %s", options.Name)
	}
	if got := options.Addrs[0]; got != "127.0.0.1:6380" {
		t.Fatalf("unexpected address: %s", got)
	}
	if options.MaxInflight != 32 {
		t.Fatalf("unexpected max inflight: %d", options.MaxInflight)
	}
	if options.Password != "secret" {
		t.Fatalf("unexpected password override: %s", options.Password)
	}
}
