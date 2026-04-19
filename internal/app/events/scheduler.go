// Package events provides deterministic clustering orchestration that consolidates extracted results into aggregated events.
package events

import (
	"context"
	"encoding/json"
	"time"

	"ai-advisor-event-aggregator/internal/logging"
)

// Scheduler runs incremental clustering on a fixed interval using the persisted clustering cursor.
type Scheduler struct {
	service  *Service
	logger   *logging.Logger
	interval time.Duration
}

// NewScheduler constructs an incremental clustering scheduler.
// The service parameter executes clustering, logger emits cycle logs, and interval controls cadence.
// It returns a configured scheduler or nil when dependencies are invalid.
func NewScheduler(service *Service, logger *logging.Logger, interval time.Duration) *Scheduler {
	if service == nil || logger == nil || interval <= 0 {
		return nil
	}
	return &Scheduler{service: service, logger: logger, interval: interval}
}

// Run starts a ticker loop that clusters new extraction results each interval.
// The ctx parameter controls graceful shutdown and cancellation of each cycle.
// It returns when context cancellation is observed.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info(ctx, "app.event_scheduler.stopped", "app/events/scheduler", "event scheduler stopped", logging.Field{Key: "reason", Value: ctx.Err().Error()})
			return
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

// runOnce executes one incremental clustering cycle and emits structured logs.
// The ctx parameter controls repository IO and merge lifecycle for this cycle.
// It has no return value because errors are logged and next cycle continues.
func (s *Scheduler) runOnce(ctx context.Context) {
	now := time.Now().UTC()
	result, err := s.service.ClusterFromStoredCursor(ctx, now)
	if err != nil {
		sanitizedInput, marshalErr := json.Marshal(map[string]any{"cycle_started_at": now.Format(time.RFC3339Nano)})
		if marshalErr != nil {
			sanitizedInput = []byte(`{"error":"sanitize_failed"}`)
		}
		s.logger.ErrorWithContract(ctx, "app.event_scheduler.cycle_failed", "app/events/scheduler", "event scheduler cycle failed", err, logging.ErrorContract{
			Failure:        "event_scheduler_cycle_failed",
			Cause:          err.Error(),
			SanitizedInput: string(sanitizedInput),
			Reaction:       "cycle aborted; retry on next tick",
		})
		return
	}
	s.logger.Info(ctx, "app.event_scheduler.cycle_completed", "app/events/scheduler", "event scheduler cycle completed",
		logging.Field{Key: "considered", Value: result.Considered},
		logging.Field{Key: "upserted", Value: result.Upserted},
	)
}
