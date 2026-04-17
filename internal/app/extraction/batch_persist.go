// Package extraction provides app-layer single and batch extraction orchestration.
// This file adds bounded-concurrency idempotent batch execution with persistence guard integration.
package extraction

import (
	"context"
	"fmt"
	"sync"

	"ai-advisor-impact-service/internal/extract"
)

// PersistBatchItemResult contains per-article idempotent extraction outcome for persistence-integrated batch runs.
type PersistBatchItemResult struct {
	ArticleID int64                  `json:"article_id"`
	Outcome   ExecutionOutcome       `json:"outcome"`
	Result    *extract.ExtractResult `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// PersistBatchResult contains ordered batch outcomes and aggregate counters.
type PersistBatchResult struct {
	Items        []PersistBatchItemResult `json:"items"`
	Total        int                      `json:"total"`
	SuccessCount int                      `json:"success_count"`
	FailureCount int                      `json:"failure_count"`
}

// RunBatchAndPersistWithOutcome executes idempotent extraction for multiple articles with bounded concurrency.
// Parameters: ctx controls lifecycle, articles is the input slice, extractor performs extraction,
// repo provides claim/state persistence, model identifies extraction model metadata, opts configures concurrency/limits.
// It returns ordered per-item outcomes and top-level setup errors only.
func RunBatchAndPersistWithOutcome(ctx context.Context, articles []Article, extractor Extractor, repo StateRepository, model string, opts BatchOptions) (PersistBatchResult, error) {
	if extractor == nil {
		return PersistBatchResult{}, fmt.Errorf("extractor is nil")
	}
	if repo == nil {
		return PersistBatchResult{}, fmt.Errorf("state repository is nil")
	}
	if model == "" {
		return PersistBatchResult{}, fmt.Errorf("model must not be empty")
	}

	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = defaultBatchConcurrency
	}
	if concurrency < 1 {
		return PersistBatchResult{}, fmt.Errorf("concurrency must be >= 1")
	}
	if opts.StopAfter < 0 {
		return PersistBatchResult{}, fmt.Errorf("stop_after must be >= 0")
	}

	limit := len(articles)
	if opts.StopAfter > 0 && opts.StopAfter < limit {
		limit = opts.StopAfter
	}
	result := PersistBatchResult{Items: make([]PersistBatchItemResult, limit), Total: limit}
	if limit == 0 {
		return result, nil
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				article := articles[idx]
				outcome, err := RunAndPersistWithOutcome(ctx, article, extractor, repo, model)
				item := PersistBatchItemResult{ArticleID: article.ID}
				if err != nil {
					item.Error = err.Error()
				} else {
					item.Outcome = outcome.Outcome
					item.Result = outcome.Result
					item.Error = outcome.Error
				}
				result.Items[idx] = item
			}
		}()
	}
	for i := 0; i < limit; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	for _, item := range result.Items {
		if item.Error == "" {
			result.SuccessCount++
		} else {
			result.FailureCount++
		}
	}
	return result, nil
}
