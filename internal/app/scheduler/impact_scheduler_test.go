package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/logging"
)

type blockingImpactRecalculator struct {
	calls atomic.Int32
	wait  chan struct{}
}

func (b *blockingImpactRecalculator) RecalculateImpacts(_ context.Context) error {
	b.calls.Add(1)
	<-b.wait
	return nil
}

// TestImpactScheduler_NoOverlappingRuns verifies overlapping impact runs are prevented.
func TestImpactScheduler_NoOverlappingRuns(t *testing.T) {
	logger := logging.NewWithWriters(&discardWriter{}, &discardWriter{})
	s := &Scheduler{
		logger: logger,
		cfg: Config{
			ImpactRecalcInterval: time.Minute,
		},
	}
	recalc := &blockingImpactRecalculator{wait: make(chan struct{})}
	s.SetImpactRecalculator(recalc)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.runImpactCycle(context.Background())
	}()
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		s.runImpactCycle(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)
	close(recalc.wait)
	wg.Wait()

	if recalc.calls.Load() != 1 {
		t.Fatalf("expected 1 impact run, got %d", recalc.calls.Load())
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
