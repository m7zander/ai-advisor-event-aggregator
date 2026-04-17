// Package events tests incremental event scheduler behavior.
package events

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/event"
	"ai-advisor-impact-service/internal/extract"
	"ai-advisor-impact-service/internal/logging"
	repopkg "ai-advisor-impact-service/internal/repository/extraction"
)

// TestScheduler_RunOnceUpdatesCursorAndLogs verifies periodic scheduler runs clustering and stores the cursor.
// It executes the scheduler with a short timeout and captures log output.
// It fails if cursor persistence or completion logging does not occur.
func TestScheduler_RunOnceUpdatesCursorAndLogs(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{events: map[string]event.Event{}, inputs: []repopkg.SuccessfulExtraction{{
		ArticleID:       10,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"us"},
		Sectors:         []string{"energy"},
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  40,
		Confidence:      0.7,
		FinishedAt:      now,
	}}}
	svc, _ := NewService(repo)

	var buf bytes.Buffer
	sched := NewScheduler(svc, logging.NewWithWriters(&buf, &buf), 10*time.Millisecond)
	if sched == nil {
		t.Fatal("expected scheduler")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	if !repo.hasCursor {
		t.Fatal("expected cursor to be persisted")
	}
	if !strings.Contains(buf.String(), "event scheduler cycle completed") {
		t.Fatalf("expected completion log, got %s", buf.String())
	}
}

type schedulerImpactRecalculator struct {
	calls int32
	err   error
}

func (s *schedulerImpactRecalculator) RecalculateImpacts(_ context.Context) error {
	atomic.AddInt32(&s.calls, 1)
	return s.err
}

func TestScheduler_RunOnceTriggersImpactRecalculation(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{events: map[string]event.Event{}, inputs: []repopkg.SuccessfulExtraction{{
		ArticleID:       11,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"us"},
		Sectors:         []string{"energy"},
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  50,
		Confidence:      0.8,
		FinishedAt:      now,
	}}}
	svc, _ := NewService(repo)
	recalc := &schedulerImpactRecalculator{}
	var buf bytes.Buffer
	sched := NewScheduler(svc, logging.NewWithWriters(&buf, &buf), 10*time.Millisecond)
	sched.SetImpactRecalculator(recalc)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	if atomic.LoadInt32(&recalc.calls) == 0 {
		t.Fatal("expected impact recalculation to be triggered")
	}
	if !strings.Contains(buf.String(), "event scheduler impact cycle completed") {
		t.Fatalf("expected impact cycle completion log, got %s", buf.String())
	}
}

func TestScheduler_RunOnceLogsImpactRecalculationError(t *testing.T) {
	repo := &fakeRepo{events: map[string]event.Event{}, inputs: []repopkg.SuccessfulExtraction{}}
	svc, _ := NewService(repo)
	recalc := &schedulerImpactRecalculator{err: fmt.Errorf("boom")}
	var buf bytes.Buffer
	sched := NewScheduler(svc, logging.NewWithWriters(&buf, &buf), 10*time.Millisecond)
	sched.SetImpactRecalculator(recalc)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	if !strings.Contains(buf.String(), "event scheduler impact cycle failed") {
		t.Fatalf("expected impact cycle failure log, got %s", buf.String())
	}
}
