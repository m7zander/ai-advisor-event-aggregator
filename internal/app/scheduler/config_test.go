// Package scheduler provides in-process polling and dispatch orchestration for automatic extraction runs.
package scheduler

import (
	"testing"
	"time"
)

// TestParseConfigFromEnvDefaults verifies default values when env vars are absent.
func TestParseConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("SCHEDULER_ENABLED", "")
	t.Setenv("SCHEDULER_POLL_INTERVAL_MS", "")
	t.Setenv("SCHEDULER_PAGE_SIZE", "")
	t.Setenv("SCHEDULER_MAX_PAGES_PER_CYCLE", "")
	t.Setenv("SCHEDULER_DISPATCH_CONCURRENCY", "")
	t.Setenv("SCHEDULER_BATCH_SIZE", "")
	t.Setenv("SCHEDULER_LOG_BATCH_IDS", "")
	t.Setenv("SCHEDULER_LOG_FAILED_ITEMS", "")
	t.Setenv("IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES", "")

	cfg, err := ParseConfigFromEnv()
	if err != nil {
		t.Fatalf("ParseConfigFromEnv() error = %v", err)
	}
	if cfg.Enabled != defaultEnabled || cfg.PollInterval != defaultPollInterval || cfg.PageSize != defaultPageSize || cfg.MaxPagesPerCycle != defaultMaxPagesPerCycle || cfg.DispatchConcurrency != defaultDispatchConcurrency || cfg.BatchSize != defaultBatchSize || cfg.LogBatchIDs != defaultLogBatchIDs || cfg.LogFailedItems != defaultLogFailedItems || cfg.ImpactRecalcInterval != defaultImpactRecalcInterval {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

// TestParseConfigFromEnvValid verifies explicit valid env values are parsed.
func TestParseConfigFromEnvValid(t *testing.T) {
	t.Setenv("SCHEDULER_ENABLED", "true")
	t.Setenv("SCHEDULER_POLL_INTERVAL_MS", "60000")
	t.Setenv("SCHEDULER_PAGE_SIZE", "25")
	t.Setenv("SCHEDULER_MAX_PAGES_PER_CYCLE", "4")
	t.Setenv("SCHEDULER_DISPATCH_CONCURRENCY", "3")
	t.Setenv("SCHEDULER_BATCH_SIZE", "8")
	t.Setenv("SCHEDULER_LOG_BATCH_IDS", "true")
	t.Setenv("SCHEDULER_LOG_FAILED_ITEMS", "true")
	t.Setenv("IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES", "15")

	cfg, err := ParseConfigFromEnv()
	if err != nil {
		t.Fatalf("ParseConfigFromEnv() error = %v", err)
	}
	if !cfg.Enabled || cfg.PollInterval != 60*time.Second || cfg.PageSize != 25 || cfg.MaxPagesPerCycle != 4 || cfg.DispatchConcurrency != 3 || cfg.BatchSize != 8 || !cfg.LogBatchIDs || !cfg.LogFailedItems || cfg.ImpactRecalcInterval != 15*time.Minute {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

// TestParseConfigFromEnvInvalid verifies malformed values are rejected.
func TestParseConfigFromEnvInvalid(t *testing.T) {
	t.Setenv("SCHEDULER_POLL_INTERVAL_MS", "x")
	if _, err := ParseConfigFromEnv(); err == nil {
		t.Fatal("ParseConfigFromEnv() error = nil, want non-nil")
	}
}

func TestParseConfigFromEnvInvalidImpactInterval(t *testing.T) {
	t.Setenv("IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES", "x")
	if _, err := ParseConfigFromEnv(); err == nil {
		t.Fatal("ParseConfigFromEnv() error = nil, want non-nil")
	}
}
