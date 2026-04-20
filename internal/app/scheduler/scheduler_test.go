// Package scheduler provides in-process polling and dispatch orchestration for automatic extraction runs.
package scheduler

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	"ai-advisor-event-aggregator/internal/logging"
)

// fakeUpstreamClient is a deterministic upstream stub for scheduler tests.
type fakeUpstreamClient struct {
	mu        sync.Mutex
	pages     map[int]UpstreamArticlesResponse
	errors    map[int]error
	callCount int
}

// ListEligibleArticles returns a configured page by offset/page-size pair.
// The ctx parameter is ignored for simplicity, and limit/offset determine page index.
// It returns a preconfigured response or error.
func (f *fakeUpstreamClient) ListEligibleArticles(_ context.Context, limit int, offset int) (UpstreamArticlesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	page := 0
	if limit > 0 {
		page = offset / limit
	}
	if err, ok := f.errors[page]; ok {
		return UpstreamArticlesResponse{}, err
	}
	if resp, ok := f.pages[page]; ok {
		return resp, nil
	}
	return UpstreamArticlesResponse{}, nil
}

// fakeRunner records dispatched batches and returns configured outcomes.
type fakeRunner struct {
	mu              sync.Mutex
	batches         [][]int64
	concurrency     []int
	result          appextraction.PersistBatchResult
	results         []appextraction.PersistBatchResult
	err             error
	active          int32
	maxActive       int32
	releaseDispatch <-chan struct{}
}

// RunBatch records one batch and returns the preconfigured result/error.
// The ctx parameter controls cancellation via caller, articles is mapped to IDs for assertions, concurrency is ignored.
// It returns the configured result and error.
func (f *fakeRunner) RunBatch(_ context.Context, articles []appextraction.Article, concurrency int) (appextraction.PersistBatchResult, error) {
	current := atomic.AddInt32(&f.active, 1)
	defer atomic.AddInt32(&f.active, -1)
	for {
		max := atomic.LoadInt32(&f.maxActive)
		if current <= max {
			break
		}
		if atomic.CompareAndSwapInt32(&f.maxActive, max, current) {
			break
		}
	}
	if f.releaseDispatch != nil {
		<-f.releaseDispatch
	}

	ids := make([]int64, 0, len(articles))
	for _, article := range articles {
		ids = append(ids, article.ID)
	}
	f.mu.Lock()
	f.batches = append(f.batches, ids)
	f.concurrency = append(f.concurrency, concurrency)
	callIndex := len(f.batches) - 1
	f.mu.Unlock()
	if callIndex < len(f.results) {
		return f.results[callIndex], f.err
	}
	return f.result, f.err
}

// TestFetchEligibleArticlesPaging verifies one cycle paging behavior across pages and empty-stop.
func TestFetchEligibleArticlesPaging(t *testing.T) {
	t.Parallel()
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: time.Now().UTC().Format(time.RFC3339)}}},
		1: {Data: []UpstreamArticle{{ID: 2, PublishedAt: time.Now().UTC().Format(time.RFC3339)}}},
		2: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{}
	s, err := New(upstream, runner, logging.NewWithWriters(bytes.NewBuffer(nil), bytes.NewBuffer(nil)), Config{PollInterval: time.Second, PageSize: 1, MaxPagesPerCycle: 10, DispatchConcurrency: 2, BatchSize: 2, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	articles, pages, err := s.fetchEligibleArticles(context.Background())
	if err != nil {
		t.Fatalf("fetchEligibleArticles() error = %v", err)
	}
	if pages != 3 {
		t.Fatalf("pages = %d, want 3", pages)
	}
	if len(articles) != 2 {
		t.Fatalf("len(articles) = %d, want 2", len(articles))
	}
}

// TestFetchEligibleArticlesMaxPages verifies max-pages cap truncates polling within one cycle.
func TestFetchEligibleArticlesMaxPages(t *testing.T) {
	t.Parallel()
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: time.Now().UTC().Format(time.RFC3339)}}},
		1: {Data: []UpstreamArticle{{ID: 2, PublishedAt: time.Now().UTC().Format(time.RFC3339)}}},
	}}
	runner := &fakeRunner{}
	s, err := New(upstream, runner, logging.NewWithWriters(bytes.NewBuffer(nil), bytes.NewBuffer(nil)), Config{PollInterval: time.Second, PageSize: 1, MaxPagesPerCycle: 1, DispatchConcurrency: 2, BatchSize: 2, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	articles, pages, err := s.fetchEligibleArticles(context.Background())
	if err != nil {
		t.Fatalf("fetchEligibleArticles() error = %v", err)
	}
	if pages != 1 {
		t.Fatalf("pages = %d, want 1", pages)
	}
	if len(articles) != 1 {
		t.Fatalf("len(articles) = %d, want 1", len(articles))
	}
}

// TestRunCycleDispatch verifies discovered IDs are dispatched in configured batch chunks.
func TestRunCycleDispatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: now}, {ID: 2, PublishedAt: now}, {ID: 3, PublishedAt: now}}},
		1: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{result: appextraction.PersistBatchResult{Items: []appextraction.PersistBatchItemResult{{Outcome: appextraction.ExecutionOutcomeNewlyExtracted}}}}
	s, err := New(upstream, runner, logging.NewWithWriters(bytes.NewBuffer(nil), bytes.NewBuffer(nil)), Config{PollInterval: time.Second, PageSize: 10, MaxPagesPerCycle: 10, DispatchConcurrency: 2, BatchSize: 2, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	stats, err := s.runCycle(context.Background())
	if err != nil {
		t.Fatalf("runCycle() error = %v", err)
	}
	if stats.DispatchBatches != 2 {
		t.Fatalf("DispatchBatches = %d, want 2", stats.DispatchBatches)
	}
	if len(runner.batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(runner.batches))
	}
	if got := runner.batches[0]; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("first batch = %v, want [1 2]", got)
	}
	if got := runner.batches[1]; len(got) != 1 || got[0] != 3 {
		t.Fatalf("second batch = %v, want [3]", got)
	}
}

// TestRunCycleRateLimitThrottleTriggered verifies cycle-level rate-limit threshold triggers throttling for follow-up batches.
func TestRunCycleRateLimitThrottleTriggered(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: now}, {ID: 2, PublishedAt: now}, {ID: 3, PublishedAt: now}}},
		1: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{
		results: []appextraction.PersistBatchResult{
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 1, Error: "non-2xx response: status=429"},
				{ArticleID: 2, Error: "rate limit reached"},
			}},
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 3, Outcome: appextraction.ExecutionOutcomeNewlyExtracted},
			}},
		},
	}
	logBuffer := bytes.NewBuffer(nil)
	s, err := New(upstream, runner, logging.NewWithWriters(logBuffer, logBuffer), Config{
		PollInterval:        time.Second,
		PageSize:            10,
		MaxPagesPerCycle:    10,
		DispatchConcurrency: 4,
		BatchSize:           2,
		RateLimitThreshold:  2,
		RateLimitCooldown:   150 * time.Millisecond,
		LogBatchIDs:         false,
		LogFailedItems:      false,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	originalWait := schedulerWait
	var waitCalls atomic.Int32
	schedulerWait = func(ctx context.Context, _ time.Duration) error {
		waitCalls.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	defer func() { schedulerWait = originalWait }()

	stats, err := s.runCycle(context.Background())
	if err != nil {
		t.Fatalf("runCycle() error = %v", err)
	}
	if !stats.ThrottleApplied {
		t.Fatalf("expected throttle to be applied, stats=%+v", stats)
	}
	if stats.RateLimitedCount != 2 {
		t.Fatalf("RateLimitedCount = %d, want 2", stats.RateLimitedCount)
	}
	if waitCalls.Load() != 1 {
		t.Fatalf("wait calls = %d, want 1", waitCalls.Load())
	}
	if len(runner.concurrency) != 2 || runner.concurrency[0] != 4 || runner.concurrency[1] != 2 {
		t.Fatalf("runner concurrency calls = %v, want [4 2]", runner.concurrency)
	}
	out := logBuffer.String()
	if !strings.Contains(out, `"event":"app.scheduler.rate_limit_throttle_started"`) {
		t.Fatalf("expected throttle start log, got: %s", out)
	}
	if !strings.Contains(out, `"event":"app.scheduler.rate_limit_throttle_completed"`) {
		t.Fatalf("expected throttle completion log, got: %s", out)
	}
}

// TestRunCycleRateLimitThrottleNotTriggered verifies no throttling is applied below threshold.
func TestRunCycleRateLimitThrottleNotTriggered(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: now}, {ID: 2, PublishedAt: now}, {ID: 3, PublishedAt: now}}},
		1: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{
		results: []appextraction.PersistBatchResult{
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 1, Error: "validation failure"},
				{ArticleID: 2, Outcome: appextraction.ExecutionOutcomeAlreadyDone},
			}},
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 3, Outcome: appextraction.ExecutionOutcomeNewlyExtracted},
			}},
		},
	}
	logBuffer := bytes.NewBuffer(nil)
	s, err := New(upstream, runner, logging.NewWithWriters(logBuffer, logBuffer), Config{
		PollInterval:        time.Second,
		PageSize:            10,
		MaxPagesPerCycle:    10,
		DispatchConcurrency: 4,
		BatchSize:           2,
		RateLimitThreshold:  2,
		RateLimitCooldown:   150 * time.Millisecond,
		LogBatchIDs:         false,
		LogFailedItems:      false,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	originalWait := schedulerWait
	var waitCalls atomic.Int32
	schedulerWait = func(_ context.Context, _ time.Duration) error {
		waitCalls.Add(1)
		return nil
	}
	defer func() { schedulerWait = originalWait }()

	stats, err := s.runCycle(context.Background())
	if err != nil {
		t.Fatalf("runCycle() error = %v", err)
	}
	if stats.ThrottleApplied {
		t.Fatalf("did not expect throttle, stats=%+v", stats)
	}
	if stats.RateLimitedCount != 0 {
		t.Fatalf("RateLimitedCount = %d, want 0", stats.RateLimitedCount)
	}
	if waitCalls.Load() != 0 {
		t.Fatalf("wait calls = %d, want 0", waitCalls.Load())
	}
	if len(runner.concurrency) != 2 || runner.concurrency[0] != 4 || runner.concurrency[1] != 4 {
		t.Fatalf("runner concurrency calls = %v, want [4 4]", runner.concurrency)
	}
	out := logBuffer.String()
	if strings.Contains(out, `"event":"app.scheduler.rate_limit_throttle_started"`) {
		t.Fatalf("did not expect throttle logs, got: %s", out)
	}
}

// TestRunCycleRateLimitThrottleIgnoresAlreadyFailed verifies historical already_failed rate-limit errors do not trigger throttle.
func TestRunCycleRateLimitThrottleIgnoresAlreadyFailed(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 1, PublishedAt: now}, {ID: 2, PublishedAt: now}, {ID: 3, PublishedAt: now}}},
		1: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{
		results: []appextraction.PersistBatchResult{
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 1, Outcome: appextraction.ExecutionOutcomeAlreadyFailed, Error: "non-2xx response: status=429"},
				{ArticleID: 2, Outcome: appextraction.ExecutionOutcomeAlreadyDone},
			}},
			{Items: []appextraction.PersistBatchItemResult{
				{ArticleID: 3, Outcome: appextraction.ExecutionOutcomeNewlyExtracted},
			}},
		},
	}
	logBuffer := bytes.NewBuffer(nil)
	s, err := New(upstream, runner, logging.NewWithWriters(logBuffer, logBuffer), Config{
		PollInterval:        time.Second,
		PageSize:            10,
		MaxPagesPerCycle:    10,
		DispatchConcurrency: 4,
		BatchSize:           2,
		RateLimitThreshold:  1,
		RateLimitCooldown:   150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	originalWait := schedulerWait
	var waitCalls atomic.Int32
	schedulerWait = func(_ context.Context, _ time.Duration) error {
		waitCalls.Add(1)
		return nil
	}
	defer func() { schedulerWait = originalWait }()

	stats, err := s.runCycle(context.Background())
	if err != nil {
		t.Fatalf("runCycle() error = %v", err)
	}
	if stats.ThrottleApplied {
		t.Fatalf("did not expect throttle for already_failed historical errors, stats=%+v", stats)
	}
	if stats.RateLimitedCount != 0 {
		t.Fatalf("RateLimitedCount = %d, want 0", stats.RateLimitedCount)
	}
	if waitCalls.Load() != 0 {
		t.Fatalf("wait calls = %d, want 0", waitCalls.Load())
	}
	if len(runner.concurrency) != 2 || runner.concurrency[0] != 4 || runner.concurrency[1] != 4 {
		t.Fatalf("runner concurrency calls = %v, want [4 4]", runner.concurrency)
	}
}

// TestRunStopsOnContextCancellation verifies Run exits when context is canceled.
func TestRunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{0: {Data: []UpstreamArticle{}}}}
	runner := &fakeRunner{}
	s, err := New(upstream, runner, logging.NewWithWriters(bytes.NewBuffer(nil), bytes.NewBuffer(nil)), Config{PollInterval: time.Hour, PageSize: 1, MaxPagesPerCycle: 1, DispatchConcurrency: 1, BatchSize: 1, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if runErr := s.Run(ctx); runErr != nil {
		t.Fatalf("Run() error = %v, want nil", runErr)
	}
}

// TestRunCycleFailureHandling verifies upstream/dispatch errors are surfaced but scheduler recovers on next cycle.
func TestRunCycleFailureHandling(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{
		pages:  map[int]UpstreamArticlesResponse{0: {Data: []UpstreamArticle{{ID: 11, PublishedAt: now}}}, 1: {Data: []UpstreamArticle{}}},
		errors: map[int]error{0: errors.New("boom")},
	}
	runner := &fakeRunner{err: errors.New("dispatch boom")}
	s, err := New(upstream, runner, logging.NewWithWriters(bytes.NewBuffer(nil), bytes.NewBuffer(nil)), Config{PollInterval: time.Second, PageSize: 10, MaxPagesPerCycle: 2, DispatchConcurrency: 1, BatchSize: 1, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, cycleErr := s.runCycle(context.Background())
	if cycleErr == nil {
		t.Fatalf("runCycle() error = nil, want non-nil")
	}
	delete(upstream.errors, 0)
	stats, cycleErr := s.runCycle(context.Background())
	if cycleErr != nil {
		t.Fatalf("runCycle() second error = %v", cycleErr)
	}
	if stats.DispatchErrors != 1 || stats.Failed == 0 {
		t.Fatalf("stats = %+v, want dispatch_errors=1 and failed>0", stats)
	}
}

// TestCountBatchOutcomes verifies outcome accounting does not double-count failed items.
func TestCountBatchOutcomes(t *testing.T) {
	t.Parallel()
	stats := CycleStats{}
	countBatchOutcomes(&stats, appextraction.PersistBatchResult{
		Items: []appextraction.PersistBatchItemResult{
			{Outcome: appextraction.ExecutionOutcomeNewlyExtracted},
			{Outcome: appextraction.ExecutionOutcomeAlreadyDone},
			{Outcome: appextraction.ExecutionOutcomeAlreadyPending},
			{Outcome: appextraction.ExecutionOutcomeAlreadyFailed, Error: "previous failure"},
			{Outcome: "", Error: "hard failure"},
		},
	})
	if stats.NewlyExtracted != 1 || stats.AlreadyDone != 1 || stats.AlreadyPending != 1 || stats.AlreadyFailed != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if got := stats.ErrorCauses["unknown|hard failure"]; got != 1 {
		t.Fatalf("ErrorCauses[unknown|hard failure] = %d, want 1", got)
	}
	if got := stats.ErrorCauses["unknown|previous failure"]; got != 1 {
		t.Fatalf("ErrorCauses[unknown|previous failure] = %d, want 1", got)
	}
	if got := stats.ErrorSampleIDs["unknown|hard failure"]; !reflect.DeepEqual(got, []int64{0}) {
		t.Fatalf("ErrorSampleIDs[unknown|hard failure] = %v, want [0]", got)
	}
}

// TestCountRateLimitedItemsIgnoresAlreadyFailed verifies old persisted failures do not inflate current cycle rate-limit counts.
func TestCountRateLimitedItemsIgnoresAlreadyFailed(t *testing.T) {
	t.Parallel()
	result := appextraction.PersistBatchResult{
		Items: []appextraction.PersistBatchItemResult{
			{ArticleID: 10, Outcome: appextraction.ExecutionOutcomeAlreadyFailed, Error: "non-2xx response: status=429"},
			{ArticleID: 11, Outcome: appextraction.ExecutionOutcomeAlreadyDone},
			{ArticleID: 12, Outcome: "", Error: "non-2xx response: status=429"},
		},
	}
	if got := countRateLimitedItems(result); got != 1 {
		t.Fatalf("countRateLimitedItems() = %d, want 1", got)
	}
}

// TestFormatCycleErrorCausesTop verifies cycle-level compact top list rendering and deterministic ordering.
func TestFormatCycleErrorCausesTop(t *testing.T) {
	t.Parallel()
	stats := CycleStats{
		ErrorCauses: map[string]int{
			"unknown|timeout":               4,
			"unknown|schema":                2,
			"llm_rate_limit|rate_limit_tpm": 2,
		},
		ErrorSampleIDs: map[string][]int64{
			"unknown|timeout":               {11, 12, 13},
			"unknown|schema":                {21},
			"llm_rate_limit|rate_limit_tpm": {31, 32},
		},
	}

	got := formatCycleErrorCausesTop(stats, 2)
	want := "1)category=unknown cause=timeout count=4(ids=[11 12 13]); 2)category=llm_rate_limit cause=rate_limit_tpm count=2(ids=[31 32])"
	if got != want {
		t.Fatalf("formatCycleErrorCausesTop() = %q, want %q", got, want)
	}

	if none := formatCycleErrorCausesTop(CycleStats{}, 5); none != "none" {
		t.Fatalf("formatCycleErrorCausesTop(empty) = %q, want none", none)
	}
}

// TestClassifyError verifies raw extraction error texts map to stable observability categories.
func TestClassifyError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "llm non-2xx", in: "run extraction: extractor call failed: non-2xx response: status=502", want: "llm_http_non_2xx"},
		{name: "llm rate limit 429", in: "run extraction: extractor call failed: non-2xx response: status=429 body={...}", want: "llm_rate_limit"},
		{name: "llm http failure", in: "run extraction: extractor call failed: http failure: timeout", want: "llm_http_failure"},
		{name: "llm json decode", in: "run extraction: extractor call failed: json decode failure: invalid character", want: "llm_json_decode"},
		{name: "extract validation", in: "run extraction: validate extract result: confidence must be <= 1", want: "extract_validation"},
		{name: "article conversion", in: "convert done record: record missing required extracted fields", want: "article_conversion"},
		{name: "persist failure", in: "persist failure state: write failed", want: "persist_failure"},
		{name: "unknown", in: "unmapped random failure", want: "unknown"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyError(tc.in); got != tc.want {
				t.Fatalf("classifyError(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeBatchErrorCause verifies grouping normalization keeps stable high-level causes.
func TestNormalizeBatchErrorCause(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "trim and prefix", in: " db timeout: context deadline exceeded ", want: "db timeout"},
		{name: "429 tpm", in: "non-2xx response: status=429 body=Rate limit reached for tokens per min", want: "rate_limit_tpm"},
		{name: "429 generic", in: "transient non-2xx response: status=429 body=something", want: "rate_limit"},
		{name: "no colon", in: "invalid payload", want: "invalid payload"},
		{name: "blank", in: "   ", want: "unknown error"},
		{name: "empty prefix", in: ": trailing", want: ": trailing"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeBatchErrorCause(tc.in); got != tc.want {
				t.Fatalf("normalizeBatchErrorCause(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestSanitizeErrorText verifies response body fragments are redacted from logged/persisted causes.
func TestSanitizeErrorText(t *testing.T) {
	t.Parallel()
	in := `non-2xx response: status=429 body={"error":{"message":"sensitive"}}`
	got := sanitizeErrorText(in)
	if strings.Contains(got, "sensitive") {
		t.Fatalf("expected sensitive body content to be redacted, got: %q", got)
	}
	if got != "non-2xx response: status=429 body=[redacted]" {
		t.Fatalf("sanitizeErrorText() = %q", got)
	}
}

// TestLogBatchErrorSummary verifies grouped error summary and optional failed-item debug logs.
func TestLogBatchErrorSummary(t *testing.T) {
	t.Parallel()
	logBuffer := bytes.NewBuffer(nil)
	s := &Scheduler{
		logger: logging.NewWithWriters(logBuffer, logBuffer),
		cfg:    Config{LogFailedItems: true},
	}

	s.logBatchErrorSummary(appextraction.PersistBatchResult{
		Items: []appextraction.PersistBatchItemResult{
			{ArticleID: 1, Outcome: "", Error: "db timeout: connect"},
			{ArticleID: 2, Outcome: "", Error: "db timeout: read"},
			{ArticleID: 3, Outcome: "", Error: "schema mismatch: field x"},
			{ArticleID: 5, Outcome: "", Error: `non-2xx response: status=429 body={"error":{"message":"sensitive"}}`},
			{ArticleID: 4, Outcome: appextraction.ExecutionOutcomeAlreadyDone, Error: ""},
		},
	})

	output := logBuffer.String()
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_error_summary"`) || !strings.Contains(output, `"total_errors":4`) {
		t.Fatalf("expected summary line in output, got: %s", output)
	}
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_error_cause"`) || !strings.Contains(output, `"cause":"db timeout"`) {
		t.Fatalf("expected top cause line in output, got: %s", output)
	}
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_failed_item"`) || !strings.Contains(output, `"article_id":3`) {
		t.Fatalf("expected failed-item debug line in output, got: %s", output)
	}
	if !strings.Contains(output, `"failure":"scheduler_dispatch_item_failed"`) {
		t.Fatalf("expected failure contract in output, got: %s", output)
	}
	if !strings.Contains(output, `"reaction":"item marked failed; batch continues"`) {
		t.Fatalf("expected reaction contract in output, got: %s", output)
	}
	if strings.Contains(output, "sensitive") {
		t.Fatalf("expected redacted cause, got: %s", output)
	}
	if !strings.Contains(output, `"cause":"non-2xx response: status=429 body=[redacted]"`) {
		t.Fatalf("expected explicit redacted 429 cause, got: %s", output)
	}
	if !strings.Contains(output, `"sanitized_input":"{\"article_id\":3,\"outcome\":\"\"}"`) {
		t.Fatalf("expected sanitized_input contract in output, got: %s", output)
	}
}

// TestRunCycleWithLoggingIncludesCycleErrorCauseTop verifies final cycle log contains compact cause top list.
func TestRunCycleWithLoggingIncludesCycleErrorCauseTop(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Format(time.RFC3339)
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{{ID: 101, PublishedAt: now}, {ID: 102, PublishedAt: now}, {ID: 103, PublishedAt: now}}},
		1: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{result: appextraction.PersistBatchResult{Items: []appextraction.PersistBatchItemResult{
		{ArticleID: 101, Error: "db timeout: connect"},
		{ArticleID: 102, Error: "db timeout: read"},
		{ArticleID: 103, Error: "schema mismatch: field"},
	}}}
	logBuffer := bytes.NewBuffer(nil)
	s, err := New(upstream, runner, logging.NewWithWriters(logBuffer, logBuffer), Config{PollInterval: time.Second, PageSize: 10, MaxPagesPerCycle: 2, DispatchConcurrency: 1, BatchSize: 10, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	s.runCycleWithLogging(context.Background())

	output := logBuffer.String()
	if !strings.Contains(output, `"event":"app.scheduler.cycle_completed"`) || !strings.Contains(output, `"discovered_count":3`) {
		t.Fatalf("expected discovered count/sample in output, got: %s", output)
	}
	if strings.Contains(output, "discovered_ids=") {
		t.Fatalf("expected removed discovered_ids payload in output, got: %s", output)
	}
	if !strings.Contains(output, `"error_causes_top":"1)category=unknown cause=db timeout count=2(ids=[101 102]); 2)category=unknown cause=schema mismatch count=1(ids=[103])"`) {
		t.Fatalf("expected cycle error cause top list in output, got: %s", output)
	}
}

// TestRunCycleWithLoggingIdle verifies idle cycle log format when no IDs are discovered.
func TestRunCycleWithLoggingIdle(t *testing.T) {
	t.Parallel()
	upstream := &fakeUpstreamClient{pages: map[int]UpstreamArticlesResponse{
		0: {Data: []UpstreamArticle{}},
	}}
	runner := &fakeRunner{}
	logBuffer := bytes.NewBuffer(nil)
	s, err := New(upstream, runner, logging.NewWithWriters(logBuffer, logBuffer), Config{PollInterval: time.Second, PageSize: 10, MaxPagesPerCycle: 1, DispatchConcurrency: 1, BatchSize: 10, RateLimitThreshold: 3, RateLimitCooldown: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	s.runCycleWithLogging(context.Background())

	output := logBuffer.String()
	if !strings.Contains(output, `"event":"app.scheduler.cycle_idle"`) || !strings.Contains(output, `"pages_fetched":1`) {
		t.Fatalf("expected idle log line in output, got: %s", output)
	}
	if strings.Contains(output, "scheduler cycle completed") {
		t.Fatalf("did not expect completed log line for idle cycle, got: %s", output)
	}
}

// TestLogBatchErrorSummarySampleIDsCap verifies sample article IDs are capped per cause.
func TestLogBatchErrorSummarySampleIDsCap(t *testing.T) {
	t.Parallel()
	logBuffer := bytes.NewBuffer(nil)
	s := &Scheduler{
		logger: logging.NewWithWriters(logBuffer, logBuffer),
		cfg:    Config{LogFailedItems: false},
	}

	items := make([]appextraction.PersistBatchItemResult, 0, 5)
	for _, id := range []int64{11, 12, 13, 14, 15} {
		items = append(items, appextraction.PersistBatchItemResult{ArticleID: id, Error: "rate limit: upstream"})
	}
	s.logBatchErrorSummary(appextraction.PersistBatchResult{Items: items})

	if strings.Contains(logBuffer.String(), "scheduler dispatch batch failed item:") {
		t.Fatalf("unexpected failed-item debug lines when disabled: %s", logBuffer.String())
	}
	if !strings.Contains(logBuffer.String(), `"sample_article_ids":[11,12,13]`) {
		t.Fatalf("expected sample IDs to be capped to first 3, got: %s", logBuffer.String())
	}
}

// TestLogDispatchBatchDefaultOmitsArticleIDs verifies default batch start/completed logs stay compact without ID lists.
func TestLogDispatchBatchDefaultOmitsArticleIDs(t *testing.T) {
	t.Parallel()
	logBuffer := bytes.NewBuffer(nil)
	s := &Scheduler{
		logger: logging.NewWithWriters(logBuffer, logBuffer),
		cfg:    Config{LogBatchIDs: false},
	}

	chunk := []int64{101, 102, 103}
	s.logDispatchBatchStarted(2, 4, chunk)
	s.logDispatchBatchCompleted(2, 4, chunk, 3, BatchOutcomeCounts{
		NewlyExtracted: 1,
		AlreadyDone:    1,
		AlreadyFailed:  1,
		Failures:       0,
	})

	output := logBuffer.String()
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_started"`) || !strings.Contains(output, `"batch_index":"2/4"`) {
		t.Fatalf("expected compact started log fields, got: %s", output)
	}
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_completed"`) || !strings.Contains(output, `"total":3`) {
		t.Fatalf("expected compact completed log fields, got: %s", output)
	}
	if strings.Contains(output, "article_ids=") {
		t.Fatalf("did not expect article_ids in default mode, got: %s", output)
	}
}

// TestLogDispatchBatchWithIDsEnabled verifies optional ID list logging when SCHEDULER_LOG_BATCH_IDS is enabled.
func TestLogDispatchBatchWithIDsEnabled(t *testing.T) {
	t.Parallel()
	logBuffer := bytes.NewBuffer(nil)
	s := &Scheduler{
		logger: logging.NewWithWriters(logBuffer, logBuffer),
		cfg:    Config{LogBatchIDs: true},
	}

	chunk := []int64{301, 302}
	s.logDispatchBatchStarted(1, 2, chunk)
	s.logDispatchBatchCompleted(1, 2, chunk, 2, BatchOutcomeCounts{NewlyExtracted: 2})

	output := logBuffer.String()
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_started"`) || !strings.Contains(output, `"article_ids":[301,302]`) {
		t.Fatalf("expected started log with article IDs, got: %s", output)
	}
	if !strings.Contains(output, `"event":"app.scheduler.dispatch_batch_completed"`) || !strings.Contains(output, `"newly_extracted":2`) {
		t.Fatalf("expected completed log with article IDs, got: %s", output)
	}
}

// TestToModelArticleID verifies valid and invalid model ID conversions.
func TestToModelArticleID(t *testing.T) {
	t.Parallel()
	if _, err := toModelArticleID(0); err == nil {
		t.Fatal("toModelArticleID(0) error=nil, want non-nil")
	}
	if strconv.IntSize == 32 {
		if _, err := toModelArticleID(math.MaxInt64); err == nil {
			t.Fatal("toModelArticleID(MaxInt64) error=nil, want non-nil on 32-bit int")
		}
	}
	value, err := toModelArticleID(123)
	if err != nil {
		t.Fatalf("toModelArticleID(123) error=%v", err)
	}
	if value != 123 {
		t.Fatalf("toModelArticleID(123)=%d, want 123", value)
	}
}

// TestChunkIDs verifies chunk splitting with deterministic ordering and boundaries.
func TestChunkIDs(t *testing.T) {
	t.Parallel()
	got := chunkIDs([]int64{1, 2, 3, 4, 5}, 2)
	want := [][]int64{{1, 2}, {3, 4}, {5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chunkIDs() = %v, want %v", got, want)
	}
}
