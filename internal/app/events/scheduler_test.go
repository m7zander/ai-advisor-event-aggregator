// Package events tests incremental event scheduler behavior.
package events

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/logging"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
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

type failingCursorRepo struct {
	fakeRepo
	cursorErr error
}

func (f *failingCursorRepo) GetClusteringCursor(_ context.Context) (time.Time, bool, error) {
	return time.Time{}, false, f.cursorErr
}

// TestScheduler_RunOnceFailureLogsContract verifies runOnce emits required error contract fields on failure.
func TestScheduler_RunOnceFailureLogsContract(t *testing.T) {
	repo := &failingCursorRepo{
		fakeRepo:  fakeRepo{events: map[string]event.Event{}},
		cursorErr: errors.New("cursor backend unavailable"),
	}
	svc, err := NewService(repo)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	var buf bytes.Buffer
	sched := NewScheduler(svc, logging.NewWithWriters(&buf, &buf), time.Second)
	if sched == nil {
		t.Fatal("expected scheduler")
	}

	sched.runOnce(context.Background())
	output := buf.String()
	if !strings.Contains(output, `"event":"app.event_scheduler.cycle_failed"`) {
		t.Fatalf("expected cycle failure event, got %s", output)
	}
	if !strings.Contains(output, `"failure":"event_scheduler_cycle_failed"`) {
		t.Fatalf("expected failure contract field, got %s", output)
	}
	if !strings.Contains(output, `"reaction":"cycle aborted; retry on next tick"`) {
		t.Fatalf("expected reaction contract field, got %s", output)
	}
	if !strings.Contains(output, `"sanitized_input":"{\"cycle_started_at\":`) {
		t.Fatalf("expected sanitized_input contract field, got %s", output)
	}
}
