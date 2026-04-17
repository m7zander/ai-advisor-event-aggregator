// Package extraction provides app-layer single and batch extraction orchestration.
// This file adds bounded-concurrency batch execution on top of the existing single-article Run flow.
package extraction

import (
	"context"
	"fmt"
	"sync"

	"ai-advisor-impact-service/internal/extract"
	"ai-advisor-impact-service/internal/observability"
)

const defaultBatchConcurrency = 2

// BatchOptions controls bounded-concurrency execution for batch extraction.
type BatchOptions struct {
	Concurrency int
	StopAfter   int
}

// BatchItemResult contains the per-article extraction outcome for batch execution.
type BatchItemResult struct {
	ArticleID int64                  `json:"article_id"`
	Result    *extract.ExtractResult `json:"result,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// BatchResult contains ordered per-item outcomes and aggregate counters.
type BatchResult struct {
	Items        []BatchItemResult `json:"items"`
	Total        int               `json:"total"`
	SuccessCount int               `json:"success_count"`
	FailureCount int               `json:"failure_count"`
}

// RunBatch executes extraction for multiple articles with bounded concurrency.
// Parameters: ctx controls execution lifecycle; articles is the input slice; extractor is the dependency used by Run;
// opts configures concurrency and optional stop-after behavior.
// It returns ordered per-item outcomes and a top-level error only for batch setup failures.
func RunBatch(ctx context.Context, articles []Article, extractor Extractor, opts BatchOptions) (BatchResult, error) {
	ctx, span := observability.StartSpan(ctx, "extraction.batch_run")
	defer span.End()

	if extractor == nil {
		err := fmt.Errorf("extractor is nil")
		observability.RecordError(span, err)
		return BatchResult{}, err
	}

	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = defaultBatchConcurrency
	}
	if concurrency < 1 {
		err := fmt.Errorf("concurrency must be >= 1")
		observability.RecordError(span, err)
		return BatchResult{}, err
	}
	if opts.StopAfter < 0 {
		err := fmt.Errorf("stop_after must be >= 0")
		observability.RecordError(span, err)
		return BatchResult{}, err
	}

	limit := len(articles)
	if opts.StopAfter > 0 && opts.StopAfter < limit {
		limit = opts.StopAfter
	}

	result := BatchResult{Items: make([]BatchItemResult, limit), Total: limit}
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
				item := BatchItemResult{ArticleID: article.ID}

				out, err := Run(ctx, article, extractor)
				if err != nil {
					item.Error = err.Error()
				} else {
					item.Result = &out
				}

				result.Items[idx] = item
			}
		}()
	}

	for idx := 0; idx < limit; idx++ {
		jobs <- idx
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
