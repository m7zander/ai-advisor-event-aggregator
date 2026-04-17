// Package extraction tests bounded-concurrency batch orchestration behavior.
package extraction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/extract"
)

// batchFakeExtractor is a controllable Extractor test double for batch tests.
type batchFakeExtractor struct {
	mu         sync.Mutex
	perArticle map[int64]extract.ExtractResult
	errs       map[int64]error
	delay      time.Duration
	active     int32
	maxActive  int32
}

// Extract returns configured result/error for the input article and tracks max concurrent calls.
// The ctx parameter controls cancellation and may short-circuit when cancelled.
// It returns either a per-article configured error/result or a default valid result.
func (f *batchFakeExtractor) Extract(ctx context.Context, in extract.ExtractInput) (extract.ExtractResult, error) {
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

	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return extract.ExtractResult{}, ctx.Err()
		case <-time.After(f.delay):
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.errs[in.ArticleID]; ok {
		return extract.ExtractResult{}, err
	}
	if out, ok := f.perArticle[in.ArticleID]; ok {
		return out, nil
	}
	return extract.ExtractResult{
		ArticleID:       in.ArticleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  10,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.6,
	}, nil
}

// makeBatchArticle builds a valid app-layer Article for batch test setup.
// The id parameter is assigned to the Article ID field.
// It returns a contract-valid article with content that survives preprocessing.
func makeBatchArticle(id int64) Article {
	content := "This article has enough words for preprocess to keep useful text content."
	return Article{
		ID:          id,
		Title:       "Title",
		Link:        "https://example.com/article",
		Source:      "source",
		PublishedAt: time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC),
		Content:     &content,
	}
}

// TestRunBatch_SuccessMultiple verifies batch success over multiple input articles.
// It executes batch with bounded concurrency and all-success extractor behavior.
// It fails if any item unexpectedly fails or ordering is not preserved.
func TestRunBatch_SuccessMultiple(t *testing.T) {
	articles := []Article{makeBatchArticle(1), makeBatchArticle(2), makeBatchArticle(3)}
	fx := &batchFakeExtractor{}

	got, err := RunBatch(context.Background(), articles, fx, BatchOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got.Total != 3 || got.SuccessCount != 3 || got.FailureCount != 0 {
		t.Fatalf("unexpected counters: %+v", got)
	}
	for i, item := range got.Items {
		if item.Error != "" || item.Result == nil {
			t.Fatalf("expected success item at index %d, got %+v", i, item)
		}
		if item.ArticleID != articles[i].ID || item.Result.ArticleID != articles[i].ID {
			t.Fatalf("expected input order preserved at index %d", i)
		}
	}
}

// TestRunBatch_IsolatesPerArticleErrors verifies one-article failure does not fail whole batch.
// It configures one extractor error while other items succeed.
// It fails if errors are not isolated per item.
func TestRunBatch_IsolatesPerArticleErrors(t *testing.T) {
	articles := []Article{makeBatchArticle(1), makeBatchArticle(2), makeBatchArticle(3)}
	fx := &batchFakeExtractor{errs: map[int64]error{2: errors.New("extract failed")}}

	got, err := RunBatch(context.Background(), articles, fx, BatchOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got.SuccessCount != 2 || got.FailureCount != 1 {
		t.Fatalf("unexpected counters: %+v", got)
	}
	if got.Items[1].Error == "" || got.Items[1].Result != nil {
		t.Fatalf("expected failure on second item, got %+v", got.Items[1])
	}
	if got.Items[0].Error != "" || got.Items[2].Error != "" {
		t.Fatal("expected surrounding items to succeed")
	}
}

// TestRunBatch_ValidatesConcurrencyAndSetup verifies top-level setup validation errors.
// It checks nil extractor and invalid concurrency options.
// It fails if invalid setup is accepted.
func TestRunBatch_ValidatesConcurrencyAndSetup(t *testing.T) {
	articles := []Article{makeBatchArticle(1)}

	if _, err := RunBatch(context.Background(), articles, nil, BatchOptions{Concurrency: 1}); err == nil {
		t.Fatal("expected nil extractor error")
	}
	fx := &batchFakeExtractor{}
	if _, err := RunBatch(context.Background(), articles, fx, BatchOptions{Concurrency: -1}); err == nil {
		t.Fatal("expected invalid concurrency error")
	}
}

// TestRunBatch_EnforcesConcurrencyLimit verifies no more than configured concurrent extractor calls are active.
// It uses artificial extractor delay to observe concurrent execution.
// It fails if observed concurrency exceeds configured limit.
func TestRunBatch_EnforcesConcurrencyLimit(t *testing.T) {
	articles := []Article{makeBatchArticle(1), makeBatchArticle(2), makeBatchArticle(3), makeBatchArticle(4)}
	fx := &batchFakeExtractor{delay: 50 * time.Millisecond}

	_, err := RunBatch(context.Background(), articles, fx, BatchOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if atomic.LoadInt32(&fx.maxActive) > 2 {
		t.Fatalf("max concurrency exceeded: got %d want <= 2", fx.maxActive)
	}
}

// TestRunBatch_EmptyInput verifies empty input batch returns empty successful result.
// It calls batch with no articles.
// It fails if empty input returns an unexpected error or non-empty output.
func TestRunBatch_EmptyInput(t *testing.T) {
	fx := &batchFakeExtractor{}
	got, err := RunBatch(context.Background(), nil, fx, BatchOptions{Concurrency: 1})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got.Total != 0 || len(got.Items) != 0 || got.SuccessCount != 0 || got.FailureCount != 0 {
		t.Fatalf("unexpected empty result: %+v", got)
	}
}

// TestRunBatch_StopAfter verifies optional stop-after limit processes only the prefix subset.
// It configures stop-after to a smaller count than input length.
// It fails if more items than configured are processed.
func TestRunBatch_StopAfter(t *testing.T) {
	articles := []Article{makeBatchArticle(1), makeBatchArticle(2), makeBatchArticle(3)}
	fx := &batchFakeExtractor{}

	got, err := RunBatch(context.Background(), articles, fx, BatchOptions{Concurrency: 2, StopAfter: 2})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got.Total != 2 || len(got.Items) != 2 {
		t.Fatalf("expected only two items processed, got %+v", got)
	}
	if got.Items[0].ArticleID != 1 || got.Items[1].ArticleID != 2 {
		t.Fatalf("unexpected processed ids: %+v", got.Items)
	}
}
