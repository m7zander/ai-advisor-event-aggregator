package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidatePostgresDSN_Valid(t *testing.T) {
	cases := []string{
		"postgres://user:pass@localhost:5432/appdb?sslmode=disable",
		"postgresql:///appdb?sslmode=disable",
		"host=localhost port=5432 user=user password=pass dbname=appdb sslmode=disable",
		"host=localhost user=user password='abc://def' dbname=appdb sslmode=disable",
	}
	for _, dsn := range cases {
		if err := validatePostgresDSN(dsn); err != nil {
			t.Fatalf("expected valid dsn %q, got error: %v", dsn, err)
		}
	}
}

func TestValidatePostgresDSN_InvalidOrMissing(t *testing.T) {
	cases := []string{"", "   ", "mysql://localhost:3306/testdb", "file::memory:"}
	for _, dsn := range cases {
		if err := validatePostgresDSN(dsn); err == nil {
			t.Fatalf("expected invalid dsn %q to fail validation", dsn)
		}
	}
}

func TestLoadHTTPServerTimeoutConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv("HTTP_SERVER_READ_HEADER_TIMEOUT", "")
	t.Setenv("HTTP_SERVER_READ_TIMEOUT", "")
	t.Setenv("HTTP_SERVER_WRITE_TIMEOUT", "")
	t.Setenv("HTTP_SERVER_IDLE_TIMEOUT", "")

	cfg, err := loadHTTPServerTimeoutConfigFromEnv()
	if err != nil {
		t.Fatalf("loadHTTPServerTimeoutConfigFromEnv() unexpected error: %v", err)
	}
	if cfg.ReadHeaderTimeout != defaultHTTPReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", cfg.ReadHeaderTimeout, defaultHTTPReadHeaderTimeout)
	}
	if cfg.ReadTimeout != defaultHTTPReadTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", cfg.ReadTimeout, defaultHTTPReadTimeout)
	}
	if cfg.WriteTimeout != defaultHTTPWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", cfg.WriteTimeout, defaultHTTPWriteTimeout)
	}
	if cfg.IdleTimeout != defaultHTTPIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", cfg.IdleTimeout, defaultHTTPIdleTimeout)
	}
}

func TestLoadHTTPServerTimeoutConfigFromEnv_Custom(t *testing.T) {
	t.Setenv("HTTP_SERVER_READ_HEADER_TIMEOUT", "3s")
	t.Setenv("HTTP_SERVER_READ_TIMEOUT", "12s")
	t.Setenv("HTTP_SERVER_WRITE_TIMEOUT", "20s")
	t.Setenv("HTTP_SERVER_IDLE_TIMEOUT", "90s")

	cfg, err := loadHTTPServerTimeoutConfigFromEnv()
	if err != nil {
		t.Fatalf("loadHTTPServerTimeoutConfigFromEnv() unexpected error: %v", err)
	}
	if cfg.ReadHeaderTimeout != 3*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v, want 3s", cfg.ReadHeaderTimeout)
	}
	if cfg.ReadTimeout != 12*time.Second {
		t.Fatalf("ReadTimeout = %v, want 12s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 20*time.Second {
		t.Fatalf("WriteTimeout = %v, want 20s", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout != 90*time.Second {
		t.Fatalf("IdleTimeout = %v, want 90s", cfg.IdleTimeout)
	}
}

func TestLoadHTTPServerTimeoutConfigFromEnv_Invalid(t *testing.T) {
	t.Setenv("HTTP_SERVER_READ_HEADER_TIMEOUT", "not-a-duration")

	_, err := loadHTTPServerTimeoutConfigFromEnv()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "HTTP_SERVER_READ_HEADER_TIMEOUT") {
		t.Fatalf("error %q does not contain env key", err)
	}
}

func TestParsePositiveDurationFromEnv_RejectsZeroAndNegative(t *testing.T) {
	t.Setenv("HTTP_SERVER_READ_TIMEOUT", "0s")
	_, err := parsePositiveDurationFromEnv("HTTP_SERVER_READ_TIMEOUT", defaultHTTPReadTimeout)
	if err == nil {
		t.Fatal("expected error for zero duration, got nil")
	}

	t.Setenv("HTTP_SERVER_READ_TIMEOUT", "-1s")
	_, err = parsePositiveDurationFromEnv("HTTP_SERVER_READ_TIMEOUT", defaultHTTPReadTimeout)
	if err == nil {
		t.Fatal("expected error for negative duration, got nil")
	}
}

func TestLoadUniverseConfigFromEnv_MissingBaseURL(t *testing.T) {
	_, err := loadUniverseConfigFromEnv("", "")
	if err == nil {
		t.Fatal("expected error for missing UNIVERSE_BASE_URL, got nil")
	}
	if !strings.Contains(err.Error(), "UNIVERSE_BASE_URL") {
		t.Fatalf("error %q does not contain env key", err)
	}
}

func TestLoadUniverseConfigFromEnv_InvalidTimeout(t *testing.T) {
	_, err := loadUniverseConfigFromEnv("https://universe.example.com", "bad")
	if err == nil {
		t.Fatal("expected error for invalid UNIVERSE_TIMEOUT_MS, got nil")
	}
	if !strings.Contains(err.Error(), "UNIVERSE_TIMEOUT_MS") {
		t.Fatalf("error %q does not contain env key", err)
	}

	_, err = loadUniverseConfigFromEnv("https://universe.example.com", "0")
	if err == nil {
		t.Fatal("expected error for non-positive UNIVERSE_TIMEOUT_MS, got nil")
	}
}

func TestLoadUniverseConfigFromEnv_Valid(t *testing.T) {
	cfg, err := loadUniverseConfigFromEnv("https://universe.example.com", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BaseURL != "https://universe.example.com" {
		t.Fatalf("BaseURL = %q, want %q", cfg.BaseURL, "https://universe.example.com")
	}
	if cfg.Timeout != defaultUniverseTimeout {
		t.Fatalf("Timeout = %v, want %v", cfg.Timeout, defaultUniverseTimeout)
	}

	cfg, err = loadUniverseConfigFromEnv("https://universe.example.com", "2500")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Timeout != 2500*time.Millisecond {
		t.Fatalf("Timeout = %v, want 2500ms", cfg.Timeout)
	}
}

func TestSanitizeURLForLog_RedactsCredentialsAndQuery(t *testing.T) {
	raw := "https://user:secret@universe.example.com/path?token=abc123&x=1#frag"
	got := sanitizeURLForLog(raw)
	if strings.Contains(got, "secret") || strings.Contains(got, "token=") || strings.Contains(got, "#frag") {
		t.Fatalf("expected sanitized URL to redact credentials/query/fragment, got %q", got)
	}
	if got != "https://universe.example.com/path" {
		t.Fatalf("sanitizeURLForLog() = %q, want %q", got, "https://universe.example.com/path")
	}
}

func TestSanitizeURLForLog_InvalidInput(t *testing.T) {
	if got := sanitizeURLForLog("://bad url"); got != "[redacted-invalid-url]" {
		t.Fatalf("sanitizeURLForLog() = %q, want %q", got, "[redacted-invalid-url]")
	}
}

func TestMigrationsDirectory_RuntimeScopeOnly(t *testing.T) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".sql" {
			t.Fatalf("runtime migrations directory must not contain root SQL files, found %q", entry.Name())
		}
	}
}

func TestMigrationsLegacyArchive_ContainsHistoricalSQL(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("migrations", "legacy_archive"))
	if err != nil {
		t.Fatalf("read legacy archive directory: %v", err)
	}

	sqlCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".sql" {
			sqlCount++
		}
	}

	if sqlCount == 0 {
		t.Fatal("expected at least one archived legacy SQL migration file")
	}
}
