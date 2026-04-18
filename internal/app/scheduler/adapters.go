// Package scheduler provides in-process polling and dispatch orchestration for automatic extraction runs.
package scheduler

import (
	"context"

	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	"ai-advisor-event-aggregator/internal/upstream"
)

// AppBatchRunner adapts existing app-layer batch extraction into scheduler's runner contract.
type AppBatchRunner struct {
	extractor appextraction.Extractor
	repo      appextraction.StateRepository
	model     string
}

// NewAppBatchRunner constructs a scheduler batch runner around existing extraction dependencies.
// The extractor parameter performs LLM extraction, repo provides persistence/idempotency, and model is metadata.
// It returns an adapter that forwards scheduler dispatch requests into RunBatchAndPersistWithOutcome.
func NewAppBatchRunner(extractor appextraction.Extractor, repo appextraction.StateRepository, model string) *AppBatchRunner {
	return &AppBatchRunner{extractor: extractor, repo: repo, model: model}
}

// RunBatch dispatches one scheduler batch into the existing app-layer extraction batch flow.
// The ctx parameter controls execution lifecycle, articles is the batch payload, and concurrency sets worker count.
// It returns per-item idempotent outcomes from the app layer or an execution error.
func (r *AppBatchRunner) RunBatch(ctx context.Context, articles []appextraction.Article, concurrency int) (appextraction.PersistBatchResult, error) {
	return appextraction.RunBatchAndPersistWithOutcome(ctx, articles, r.extractor, r.repo, r.model, appextraction.BatchOptions{Concurrency: concurrency})
}

// UpstreamClientAdapter adapts the upstream HTTP client to scheduler's upstream contract.
type UpstreamClientAdapter struct {
	client *upstream.Client
}

// NewUpstreamClientAdapter constructs an upstream adapter used by the scheduler.
// The client parameter must be an initialized upstream client.
// It returns an adapter that maps client responses into scheduler DTOs.
func NewUpstreamClientAdapter(client *upstream.Client) *UpstreamClientAdapter {
	return &UpstreamClientAdapter{client: client}
}

// ListEligibleArticles fetches one upstream page and maps it into scheduler DTOs.
// The ctx parameter controls cancellation, and limit/offset define paging controls.
// It returns mapped page data or an error from request/decode failures.
func (a *UpstreamClientAdapter) ListEligibleArticles(ctx context.Context, limit int, offset int) (UpstreamArticlesResponse, error) {
	resp, err := a.client.ListEligibleArticles(ctx, limit, offset)
	if err != nil {
		return UpstreamArticlesResponse{}, err
	}
	out := UpstreamArticlesResponse{Data: make([]UpstreamArticle, 0, len(resp.Data))}
	for _, item := range resp.Data {
		out.Data = append(out.Data, UpstreamArticle{
			ID:                  int64(item.ID),
			Title:               item.Title,
			Link:                item.Link,
			Source:              item.Source,
			Description:         item.Description,
			Content:             item.Content,
			PublishedAt:         item.PublishedAt,
			FulltextExcerpt:     item.FulltextExcerpt,
			FulltextContentText: item.FulltextContentText,
		})
	}
	return out, nil
}
