// Package scheduler provides in-process polling and dispatch orchestration for automatic extraction runs.
package scheduler

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultEnabled              = false
	defaultPollInterval         = 30 * time.Second
	defaultPageSize             = 50
	defaultMaxPagesPerCycle     = 10
	defaultDispatchConcurrency  = 2
	defaultBatchSize            = 20
	defaultLogBatchIDs          = false
	defaultLogFailedItems       = false
)

// Config defines runtime controls for the in-process scheduler.
type Config struct {
	Enabled              bool
	PollInterval         time.Duration
	PageSize             int
	MaxPagesPerCycle     int
	DispatchConcurrency  int
	BatchSize            int
	LogBatchIDs          bool
	LogFailedItems       bool
}

// ParseConfigFromEnv reads scheduler settings from environment variables.
// It loads SCHEDULER_* keys, applies defaults for optional values, and validates constraints.
// It returns a validated Config or an error when any value is malformed or out of range.
func ParseConfigFromEnv() (Config, error) {
	cfg := Config{
		Enabled:              defaultEnabled,
		PollInterval:         defaultPollInterval,
		PageSize:             defaultPageSize,
		MaxPagesPerCycle:     defaultMaxPagesPerCycle,
		DispatchConcurrency:  defaultDispatchConcurrency,
		BatchSize:            defaultBatchSize,
		LogBatchIDs:          defaultLogBatchIDs,
		LogFailedItems:       defaultLogFailedItems,
	}

	enabledRaw := os.Getenv("SCHEDULER_ENABLED")
	if enabledRaw != "" {
		enabled, err := strconv.ParseBool(enabledRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parse SCHEDULER_ENABLED: %w", err)
		}
		cfg.Enabled = enabled
	}

	pollMSRaw := os.Getenv("SCHEDULER_POLL_INTERVAL_MS")
	if pollMSRaw != "" {
		pollMS, err := strconv.Atoi(pollMSRaw)
		if err != nil || pollMS <= 0 {
			return Config{}, fmt.Errorf("SCHEDULER_POLL_INTERVAL_MS must be > 0")
		}
		cfg.PollInterval = time.Duration(pollMS) * time.Millisecond
	}

	pageSizeRaw := os.Getenv("SCHEDULER_PAGE_SIZE")
	if pageSizeRaw != "" {
		pageSize, err := strconv.Atoi(pageSizeRaw)
		if err != nil || pageSize <= 0 {
			return Config{}, fmt.Errorf("SCHEDULER_PAGE_SIZE must be > 0")
		}
		cfg.PageSize = pageSize
	}

	maxPagesRaw := os.Getenv("SCHEDULER_MAX_PAGES_PER_CYCLE")
	if maxPagesRaw != "" {
		maxPages, err := strconv.Atoi(maxPagesRaw)
		if err != nil || maxPages <= 0 {
			return Config{}, fmt.Errorf("SCHEDULER_MAX_PAGES_PER_CYCLE must be > 0")
		}
		cfg.MaxPagesPerCycle = maxPages
	}

	concurrencyRaw := os.Getenv("SCHEDULER_DISPATCH_CONCURRENCY")
	if concurrencyRaw != "" {
		concurrency, err := strconv.Atoi(concurrencyRaw)
		if err != nil || concurrency <= 0 {
			return Config{}, fmt.Errorf("SCHEDULER_DISPATCH_CONCURRENCY must be > 0")
		}
		cfg.DispatchConcurrency = concurrency
	}

	batchSizeRaw := os.Getenv("SCHEDULER_BATCH_SIZE")
	if batchSizeRaw != "" {
		batchSize, err := strconv.Atoi(batchSizeRaw)
		if err != nil || batchSize <= 0 {
			return Config{}, fmt.Errorf("SCHEDULER_BATCH_SIZE must be > 0")
		}
		cfg.BatchSize = batchSize
	}

	logFailedItemsRaw := os.Getenv("SCHEDULER_LOG_FAILED_ITEMS")
	if logFailedItemsRaw != "" {
		logFailedItems, err := strconv.ParseBool(logFailedItemsRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parse SCHEDULER_LOG_FAILED_ITEMS: %w", err)
		}
		cfg.LogFailedItems = logFailedItems
	}

	logBatchIDsRaw := os.Getenv("SCHEDULER_LOG_BATCH_IDS")
	if logBatchIDsRaw != "" {
		logBatchIDs, err := strconv.ParseBool(logBatchIDsRaw)
		if err != nil {
			return Config{}, fmt.Errorf("parse SCHEDULER_LOG_BATCH_IDS: %w", err)
		}
		cfg.LogBatchIDs = logBatchIDs
	}

	return cfg, nil
}
