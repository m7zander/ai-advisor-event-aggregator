// Package scheduler provides in-process polling and dispatch orchestration for automatic extraction runs.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	"ai-advisor-event-aggregator/internal/logging"
	"ai-advisor-event-aggregator/internal/model"
)

var timeNewTicker = time.NewTicker
var schedulerWait = waitForDuration

const (
	batchErrorSummaryTopN      = 5
	batchErrorSummarySampleIDs = 3
	discoveredSampleSize       = 5
)

// BatchOutcomeCounts captures compact per-batch execution outcome counters for scheduler logs.
type BatchOutcomeCounts struct {
	NewlyExtracted int
	AlreadyDone    int
	AlreadyPending int
	AlreadyFailed  int
	Failures       int
}

// UpstreamClient describes paging access to eligible upstream articles.
type UpstreamClient interface {
	ListEligibleArticles(ctx context.Context, limit int, offset int) (UpstreamArticlesResponse, error)
}

// ExtractionBatchRunner describes batch dispatch into the existing app-layer extraction flow.
type ExtractionBatchRunner interface {
	RunBatch(ctx context.Context, articles []appextraction.Article, concurrency int) (appextraction.PersistBatchResult, error)
}

// UpstreamArticlesResponse is the polling DTO returned by the upstream client.
type UpstreamArticlesResponse struct {
	Data []UpstreamArticle `json:"data"`
}

// UpstreamArticle is the scheduler-facing DTO for upstream articles.
type UpstreamArticle struct {
	ID                  int64   `json:"id"`
	Title               string  `json:"title"`
	Link                string  `json:"link"`
	Source              string  `json:"source"`
	Description         *string `json:"description"`
	Content             *string `json:"content"`
	PublishedAt         string  `json:"published_at"`
	FulltextExcerpt     *string `json:"fulltext_excerpt"`
	FulltextContentText *string `json:"fulltext_content_text"`
}

// Scheduler continuously polls upstream and dispatches discovered article IDs for extraction.
type Scheduler struct {
	upstreamClient UpstreamClient
	runner         ExtractionBatchRunner
	logger         *logging.Logger
	cfg            Config
}

// CycleStats captures one scheduler cycle aggregate accounting.
type CycleStats struct {
	PagesFetched     int
	ArticleCount     int
	DispatchBatches  int
	NewlyExtracted   int
	AlreadyDone      int
	AlreadyPending   int
	AlreadyFailed    int
	Failed           int
	DispatchErrors   int
	ErrorCauses      map[string]int
	ErrorSampleIDs   map[string][]int64
	DiscoveredIDs    []int64
	DispatchedIDSize int
	RateLimitedCount int
	ThrottleApplied  bool
}

// New constructs a scheduler with explicit dependencies.
// The upstreamClient parameter provides upstream polling, runner performs app-layer dispatch,
// logger emits operational logs, and cfg configures loop behavior.
// It returns a scheduler or an error when any dependency/config is invalid.
func New(upstreamClient UpstreamClient, runner ExtractionBatchRunner, logger *logging.Logger, cfg Config) (*Scheduler, error) {
	if upstreamClient == nil {
		return nil, fmt.Errorf("upstream client is nil")
	}
	if runner == nil {
		return nil, fmt.Errorf("runner is nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	if cfg.PollInterval <= 0 {
		return nil, fmt.Errorf("poll interval must be > 0")
	}
	if cfg.PageSize <= 0 || cfg.MaxPagesPerCycle <= 0 || cfg.DispatchConcurrency <= 0 || cfg.BatchSize <= 0 || cfg.RateLimitThreshold <= 0 || cfg.RateLimitCooldown <= 0 {
		return nil, fmt.Errorf("invalid scheduler numeric config")
	}
	return &Scheduler{upstreamClient: upstreamClient, runner: runner, logger: logger, cfg: cfg}, nil
}

// Run starts the continuous scheduler loop and blocks until context cancellation.
// The ctx parameter controls graceful shutdown and cycle cancellation.
// It returns nil when the context is canceled, or an error for unrecoverable setup failures.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := timeNewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	s.runCycleWithLogging(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info(ctx, "app.scheduler.loop_stopped", "app/scheduler", "scheduler loop stopped", logging.Field{Key: "reason", Value: ctx.Err().Error()})
			return nil
		case <-ticker.C:
			s.runCycleWithLogging(ctx)
		}
	}
}

// runCycleWithLogging executes one cycle and emits final success/failure logging.
// The ctx parameter controls upstream calls and dispatch lifecycle.
// It has no return value because cycle errors are logged and recovered on next loop.
func (s *Scheduler) runCycleWithLogging(ctx context.Context) {
	s.logger.Info(ctx, "app.scheduler.cycle_started", "app/scheduler", "scheduler cycle started")
	stats, err := s.runCycle(ctx)
	if err != nil {
		sanitizedInput := mustJSON(map[string]any{
			"max_pages_per_cycle":  s.cfg.MaxPagesPerCycle,
			"page_size":            s.cfg.PageSize,
			"dispatch_concurrency": s.cfg.DispatchConcurrency,
			"batch_size":           s.cfg.BatchSize,
		})
		s.logger.ErrorWithContract(ctx, "app.scheduler.cycle_failed", "app/scheduler", "scheduler cycle failed", err, logging.ErrorContract{
			Failure:        "scheduler_cycle_failed",
			Cause:          err.Error(),
			SanitizedInput: sanitizedInput,
			Reaction:       "cycle aborted; retry on next tick",
		})
		return
	}
	discoveredCount := len(stats.DiscoveredIDs)
	if discoveredCount == 0 {
		s.logger.Info(ctx, "app.scheduler.cycle_idle", "app/scheduler", "scheduler cycle idle",
			logging.Field{Key: "pages_fetched", Value: stats.PagesFetched},
			logging.Field{Key: "article_count", Value: stats.ArticleCount},
			logging.Field{Key: "discovered_count", Value: 0},
		)
		return
	}

	discoveredSample := compactDiscoveredSample(stats.DiscoveredIDs, discoveredSampleSize)
	s.logger.Info(ctx, "app.scheduler.cycle_completed", "app/scheduler", "scheduler cycle completed",
		logging.Field{Key: "pages_fetched", Value: stats.PagesFetched},
		logging.Field{Key: "article_count", Value: stats.ArticleCount},
		logging.Field{Key: "discovered_count", Value: discoveredCount},
		logging.Field{Key: "discovered_sample", Value: discoveredSample},
		logging.Field{Key: "dispatch_batches", Value: stats.DispatchBatches},
		logging.Field{Key: "dispatched_ids", Value: stats.DispatchedIDSize},
		logging.Field{Key: "newly_extracted", Value: stats.NewlyExtracted},
		logging.Field{Key: "already_done", Value: stats.AlreadyDone},
		logging.Field{Key: "already_pending", Value: stats.AlreadyPending},
		logging.Field{Key: "already_failed", Value: stats.AlreadyFailed},
		logging.Field{Key: "failed", Value: stats.Failed},
		logging.Field{Key: "dispatch_errors", Value: stats.DispatchErrors},
		logging.Field{Key: "rate_limited_count", Value: stats.RateLimitedCount},
		logging.Field{Key: "throttle_applied", Value: stats.ThrottleApplied},
		logging.Field{Key: "error_causes_top", Value: formatCycleErrorCausesTop(stats, batchErrorSummaryTopN)},
	)
}

// compactDiscoveredSample returns a bounded prefix sample of discovered article IDs for concise cycle logs.
// The ids parameter is the full discovered ID list and sampleSize limits how many IDs are included in the sample.
// It returns at most sampleSize IDs preserving order, or nil when ids is empty or sampleSize is non-positive.
func compactDiscoveredSample(ids []int64, sampleSize int) []int64 {
	if len(ids) == 0 || sampleSize <= 0 {
		return nil
	}
	if len(ids) <= sampleSize {
		return ids
	}
	return ids[:sampleSize]
}

// runCycle executes one polling + dispatch cycle.
// The ctx parameter controls cancellation for all network and app-layer work in the cycle.
// It returns aggregate cycle stats and an error when upstream paging fails.
func (s *Scheduler) runCycle(ctx context.Context) (CycleStats, error) {
	articles, pagesFetched, err := s.fetchEligibleArticles(ctx)
	if err != nil {
		return CycleStats{}, err
	}
	if len(articles) == 0 {
		return CycleStats{PagesFetched: pagesFetched}, nil
	}

	stats := CycleStats{PagesFetched: pagesFetched, ArticleCount: len(articles)}
	ids := make([]int64, 0, len(articles))
	for _, article := range articles {
		ids = append(ids, article.ID)
	}
	stats.DiscoveredIDs = ids

	batches := chunkIDs(ids, s.cfg.BatchSize)
	effectiveConcurrency := s.cfg.DispatchConcurrency
	throttleActive := false
	for batchIdx, chunk := range batches {
		if throttleActive {
			appliedCooldown := s.cfg.RateLimitCooldown
			s.logger.Info(ctx, "app.scheduler.rate_limit_throttle_started", "app/scheduler", "scheduler rate-limit throttling started",
				logging.Field{Key: "rate_limited_count", Value: stats.RateLimitedCount},
				logging.Field{Key: "applied_cooldown_ms", Value: appliedCooldown.Milliseconds()},
				logging.Field{Key: "effective_concurrency", Value: effectiveConcurrency},
			)
			if err := schedulerWait(ctx, appliedCooldown); err != nil {
				return stats, fmt.Errorf("rate-limit throttle wait canceled: %w", err)
			}
			s.logger.Info(ctx, "app.scheduler.rate_limit_throttle_completed", "app/scheduler", "scheduler rate-limit throttling completed",
				logging.Field{Key: "rate_limited_count", Value: stats.RateLimitedCount},
				logging.Field{Key: "applied_cooldown_ms", Value: appliedCooldown.Milliseconds()},
				logging.Field{Key: "effective_concurrency", Value: effectiveConcurrency},
			)
		}

		batchNumber := batchIdx + 1
		batchArticles, convErr := s.buildBatchArticles(chunk, articles)
		if convErr != nil {
			stats.Failed += len(chunk)
			stats.DispatchErrors++
			s.logger.ErrorWithContract(ctx, "app.scheduler.dispatch_batch_failed", "app/scheduler", "scheduler dispatch batch failed", convErr, logging.ErrorContract{
				Failure:        "scheduler_dispatch_batch_failed",
				Cause:          convErr.Error(),
				SanitizedInput: mustJSON(map[string]any{"article_ids": chunk, "batch_size": len(chunk)}),
				Reaction:       "batch skipped; cycle continues",
			}, logging.Field{Key: "article_ids", Value: chunk})
			continue
		}

		stats.DispatchBatches++
		stats.DispatchedIDSize += len(chunk)
		s.logDispatchBatchStarted(batchNumber, len(batches), chunk)
		result, dispatchErr := s.runner.RunBatch(ctx, batchArticles, effectiveConcurrency)
		if dispatchErr != nil {
			stats.Failed += len(chunk)
			stats.DispatchErrors++
			if isRateLimitedError(dispatchErr.Error()) {
				stats.RateLimitedCount += len(chunk)
			}
			s.logger.ErrorWithContract(ctx, "app.scheduler.dispatch_batch_failed", "app/scheduler", "scheduler dispatch batch failed", dispatchErr, logging.ErrorContract{
				Failure:        "scheduler_dispatch_batch_failed",
				Cause:          dispatchErr.Error(),
				SanitizedInput: mustJSON(map[string]any{"article_ids": chunk, "batch_size": len(chunk), "dispatch_concurrency": effectiveConcurrency}),
				Reaction:       "batch skipped; cycle continues",
			}, logging.Field{Key: "article_ids", Value: chunk})
			if !throttleActive && stats.RateLimitedCount >= s.cfg.RateLimitThreshold {
				throttleActive = true
				stats.ThrottleApplied = true
				effectiveConcurrency = throttledConcurrency(s.cfg.DispatchConcurrency)
			}
			continue
		}
		countBatchOutcomes(&stats, result)
		stats.RateLimitedCount += countRateLimitedItems(result)
		if !throttleActive && stats.RateLimitedCount >= s.cfg.RateLimitThreshold {
			throttleActive = true
			stats.ThrottleApplied = true
			effectiveConcurrency = throttledConcurrency(s.cfg.DispatchConcurrency)
		}
		s.logBatchErrorSummary(result)
		outcomeCounts := summarizeBatchOutcomes(result)
		s.logDispatchBatchCompleted(batchNumber, len(batches), chunk, result.Total, outcomeCounts)
	}

	return stats, nil
}

func throttledConcurrency(base int) int {
	if base <= 1 {
		return 1
	}
	return (base + 1) / 2
}

func countRateLimitedItems(result appextraction.PersistBatchResult) int {
	count := 0
	for _, item := range result.Items {
		if item.Outcome == appextraction.ExecutionOutcomeAlreadyFailed {
			continue
		}
		if isRateLimitedError(item.Error) {
			count++
		}
	}
	return count
}

func isRateLimitedError(errText string) bool {
	normalized := strings.ToLower(strings.TrimSpace(errText))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "status=429") ||
		strings.Contains(normalized, "rate limit") ||
		strings.Contains(normalized, "too many requests")
}

func waitForDuration(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// logDispatchBatchStarted logs compact batch dispatch start metadata and optional batch IDs.
// The batchNumber parameter is the 1-based index in the cycle, totalBatches is the full cycle batch count,
// and articleIDs is the chunk currently dispatched.
// It has no return value and writes one scheduler log line.
func (s *Scheduler) logDispatchBatchStarted(batchNumber int, totalBatches int, articleIDs []int64) {
	batchIndex := fmt.Sprintf("%d/%d", batchNumber, totalBatches)
	if s.cfg.LogBatchIDs {
		s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_started", "app/scheduler", "scheduler dispatch batch started",
			logging.Field{Key: "batch_index", Value: batchIndex},
			logging.Field{Key: "batch_size", Value: len(articleIDs)},
			logging.Field{Key: "article_ids", Value: articleIDs},
		)
		return
	}
	s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_started", "app/scheduler", "scheduler dispatch batch started",
		logging.Field{Key: "batch_index", Value: batchIndex},
		logging.Field{Key: "batch_size", Value: len(articleIDs)},
	)
}

// logDispatchBatchCompleted logs compact batch completion counters and optional batch IDs.
// The batchNumber parameter is the 1-based index in the cycle, totalBatches is the full cycle batch count,
// articleIDs is the chunk that was processed, total is the batch result size, and counts are per-outcome totals.
// It has no return value and writes one scheduler log line.
func (s *Scheduler) logDispatchBatchCompleted(batchNumber int, totalBatches int, articleIDs []int64, total int, counts BatchOutcomeCounts) {
	batchIndex := fmt.Sprintf("%d/%d", batchNumber, totalBatches)
	if s.cfg.LogBatchIDs {
		s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_completed", "app/scheduler", "scheduler dispatch batch completed",
			logging.Field{Key: "batch_index", Value: batchIndex},
			logging.Field{Key: "batch_size", Value: len(articleIDs)},
			logging.Field{Key: "total", Value: total},
			logging.Field{Key: "failures", Value: counts.Failures},
			logging.Field{Key: "already_done", Value: counts.AlreadyDone},
			logging.Field{Key: "already_failed", Value: counts.AlreadyFailed},
			logging.Field{Key: "newly_extracted", Value: counts.NewlyExtracted},
			logging.Field{Key: "already_pending", Value: counts.AlreadyPending},
			logging.Field{Key: "article_ids", Value: articleIDs},
		)
		return
	}
	s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_completed", "app/scheduler", "scheduler dispatch batch completed",
		logging.Field{Key: "batch_index", Value: batchIndex},
		logging.Field{Key: "batch_size", Value: len(articleIDs)},
		logging.Field{Key: "total", Value: total},
		logging.Field{Key: "failures", Value: counts.Failures},
		logging.Field{Key: "already_done", Value: counts.AlreadyDone},
		logging.Field{Key: "already_failed", Value: counts.AlreadyFailed},
		logging.Field{Key: "newly_extracted", Value: counts.NewlyExtracted},
		logging.Field{Key: "already_pending", Value: counts.AlreadyPending},
	)
}

// fetchEligibleArticles pages through eligible upstream articles for one cycle.
// The ctx parameter controls cancellation for all page requests.
// It returns all discovered articles, number of pages fetched, and an error when a page fetch fails.
func (s *Scheduler) fetchEligibleArticles(ctx context.Context) ([]UpstreamArticle, int, error) {
	collected := make([]UpstreamArticle, 0)
	offset := 0
	pagesFetched := 0

	for page := 0; page < s.cfg.MaxPagesPerCycle; page++ {
		resp, err := s.upstreamClient.ListEligibleArticles(ctx, s.cfg.PageSize, offset)
		if err != nil {
			s.logger.ErrorWithContract(ctx, "app.scheduler.upstream_fetch_failed", "app/scheduler", "scheduler upstream fetch failure", err, logging.ErrorContract{
				Failure:        "scheduler_upstream_fetch_failed",
				Cause:          err.Error(),
				SanitizedInput: mustJSON(map[string]any{"page": page + 1, "offset": offset, "page_size": s.cfg.PageSize}),
				Reaction:       "cycle aborted; retry on next tick",
			},
				logging.Field{Key: "page", Value: page + 1},
				logging.Field{Key: "offset", Value: offset},
			)
			return nil, pagesFetched, fmt.Errorf("fetch upstream page %d: %w", page+1, err)
		}
		pagesFetched++
		s.logger.Info(ctx, "app.scheduler.upstream_page_fetched", "app/scheduler", "scheduler upstream page fetched",
			logging.Field{Key: "page", Value: page + 1},
			logging.Field{Key: "offset", Value: offset},
			logging.Field{Key: "item_count", Value: len(resp.Data)},
		)
		if len(resp.Data) == 0 {
			break
		}
		collected = append(collected, resp.Data...)
		offset += s.cfg.PageSize
	}

	return collected, pagesFetched, nil
}

// buildBatchArticles maps a chunk of IDs into app-layer article objects.
// The ids parameter controls chunk order, and source carries upstream article payloads for conversion.
// It returns converted articles aligned with ids order or an error if any ID is missing/invalid.
func (s *Scheduler) buildBatchArticles(ids []int64, source []UpstreamArticle) ([]appextraction.Article, error) {
	byID := make(map[int64]UpstreamArticle, len(source))
	for _, article := range source {
		byID[article.ID] = article
	}

	out := make([]appextraction.Article, 0, len(ids))
	for _, id := range ids {
		article, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("article_id=%d not found in fetched source", id)
		}
		modelID, err := toModelArticleID(article.ID)
		if err != nil {
			return nil, err
		}
		converted, err := appextraction.FromModelArticle(model.Article{
			ID:                  modelID,
			Title:               article.Title,
			Link:                article.Link,
			Source:              article.Source,
			Description:         article.Description,
			Content:             article.Content,
			PublishedAt:         article.PublishedAt,
			FulltextExcerpt:     article.FulltextExcerpt,
			FulltextContentText: article.FulltextContentText,
		})
		if err != nil {
			return nil, fmt.Errorf("convert article_id=%d: %w", id, err)
		}
		out = append(out, converted)
	}
	return out, nil
}

// toModelArticleID validates and converts scheduler article IDs to model.Article ID type.
// The upstreamID parameter is an int64 identifier returned by upstream polling.
// It returns a safe int value for model.Article or an error when the ID is non-positive or out of int range.
func toModelArticleID(upstreamID int64) (int, error) {
	if upstreamID <= 0 {
		return 0, fmt.Errorf("article_id=%d must be > 0", upstreamID)
	}
	if upstreamID > math.MaxInt {
		return 0, fmt.Errorf("article_id=%d exceeds supported integer range", upstreamID)
	}
	return int(upstreamID), nil
}

// countBatchOutcomes updates cycle counters based on app-layer idempotent outcomes.
// The stats parameter is mutated in place, while result is read-only batch output.
// It has no return value.
func countBatchOutcomes(stats *CycleStats, result appextraction.PersistBatchResult) {
	if stats.ErrorCauses == nil {
		stats.ErrorCauses = make(map[string]int)
	}
	if stats.ErrorSampleIDs == nil {
		stats.ErrorSampleIDs = make(map[string][]int64)
	}

	for _, item := range result.Items {
		if item.Error != "" {
			cause := normalizeBatchErrorCause(item.Error)
			category := classifyError(item.Error)
			key := buildErrorCauseKey(category, cause)
			stats.ErrorCauses[key]++
			samples := stats.ErrorSampleIDs[key]
			if len(samples) < batchErrorSummarySampleIDs {
				stats.ErrorSampleIDs[key] = append(samples, item.ArticleID)
			}
		}

		if item.Outcome == appextraction.ExecutionOutcomeAlreadyFailed {
			stats.AlreadyFailed++
			continue
		}
		if item.Error != "" {
			stats.Failed++
			continue
		}

		switch item.Outcome {
		case appextraction.ExecutionOutcomeNewlyExtracted:
			stats.NewlyExtracted++
		case appextraction.ExecutionOutcomeAlreadyDone:
			stats.AlreadyDone++
		case appextraction.ExecutionOutcomeAlreadyPending:
			stats.AlreadyPending++
		default:
			stats.Failed++
		}
	}
}

// summarizeBatchOutcomes counts per-outcome values in one batch result for compact logging fields.
// The result parameter is the app-layer batch output for one scheduler chunk.
// It returns outcome counters, where Failures captures explicit item errors and unknown outcomes.
func summarizeBatchOutcomes(result appextraction.PersistBatchResult) BatchOutcomeCounts {
	counts := BatchOutcomeCounts{}
	for _, item := range result.Items {
		if item.Outcome == appextraction.ExecutionOutcomeAlreadyFailed {
			counts.AlreadyFailed++
			continue
		}
		if item.Error != "" {
			counts.Failures++
			continue
		}

		switch item.Outcome {
		case appextraction.ExecutionOutcomeNewlyExtracted:
			counts.NewlyExtracted++
		case appextraction.ExecutionOutcomeAlreadyDone:
			counts.AlreadyDone++
		case appextraction.ExecutionOutcomeAlreadyPending:
			counts.AlreadyPending++
		default:
			counts.Failures++
		}
	}
	return counts
}

// formatCycleErrorCausesTop builds a compact deterministic top list for cycle-level error cause logging.
// The stats parameter provides aggregated cause counters and sample IDs for one full scheduler cycle.
// The topN parameter hard-limits output size so cycle-completed logs stay concise in production log streams.
// It returns "none" when no grouped causes exist, otherwise a semicolon-separated rank list.
func formatCycleErrorCausesTop(stats CycleStats, topN int) string {
	if len(stats.ErrorCauses) == 0 {
		return "none"
	}

	type causeEntry struct {
		cause string
		count int
	}
	ordered := make([]causeEntry, 0, len(stats.ErrorCauses))
	for cause, count := range stats.ErrorCauses {
		ordered = append(ordered, causeEntry{cause: cause, count: count})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].count == ordered[j].count {
			return ordered[i].cause < ordered[j].cause
		}
		return ordered[i].count > ordered[j].count
	})

	if topN <= 0 || topN > len(ordered) {
		topN = len(ordered)
	}

	parts := make([]string, 0, topN)
	for idx := 0; idx < topN; idx++ {
		entry := ordered[idx]
		category, cause := parseErrorCauseKey(entry.cause)
		sampleIDs := stats.ErrorSampleIDs[entry.cause]
		parts = append(parts, fmt.Sprintf("%d)category=%s cause=%s count=%d(ids=%v)", idx+1, category, cause, entry.count, sampleIDs))
	}
	return strings.Join(parts, "; ")
}

// batchErrorAggregate tracks grouped batch item errors by normalized cause.
type batchErrorAggregate struct {
	cause     string
	count     int
	sampleIDs []int64
}

// logBatchErrorSummary aggregates and logs failed item errors for one dispatched batch.
// The result parameter provides per-item outcome/error details emitted by RunBatch.
// It has no return value and writes summary/debug lines to the scheduler logger.
func (s *Scheduler) logBatchErrorSummary(result appextraction.PersistBatchResult) {
	aggregates := make(map[string]*batchErrorAggregate)
	totalErrors := 0

	for _, item := range result.Items {
		if item.Error == "" {
			continue
		}

		totalErrors++
		cause := normalizeBatchErrorCause(item.Error)
		category := classifyError(item.Error)
		key := buildErrorCauseKey(category, cause)
		aggregate, ok := aggregates[key]
		if !ok {
			aggregate = &batchErrorAggregate{cause: key}
			aggregates[key] = aggregate
		}

		aggregate.count++
		if len(aggregate.sampleIDs) < batchErrorSummarySampleIDs {
			aggregate.sampleIDs = append(aggregate.sampleIDs, item.ArticleID)
		}

		if s.cfg.LogFailedItems {
			sanitizedError := sanitizeErrorText(item.Error)
			s.logger.ErrorWithContract(context.Background(), "app.scheduler.dispatch_batch_failed_item", "app/scheduler", "scheduler dispatch batch failed item", fmt.Errorf("%s", sanitizedError), logging.ErrorContract{
				Failure:        "scheduler_dispatch_item_failed",
				Cause:          sanitizedError,
				SanitizedInput: mustJSON(map[string]any{"article_id": item.ArticleID, "outcome": item.Outcome}),
				Reaction:       "item marked failed; batch continues",
			},
				logging.Field{Key: "article_id", Value: item.ArticleID},
				logging.Field{Key: "outcome", Value: item.Outcome},
			)
		}
	}

	if totalErrors == 0 {
		return
	}

	ordered := make([]batchErrorAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		ordered = append(ordered, *aggregate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].count == ordered[j].count {
			return ordered[i].cause < ordered[j].cause
		}
		return ordered[i].count > ordered[j].count
	})

	limit := batchErrorSummaryTopN
	if limit > len(ordered) {
		limit = len(ordered)
	}

	s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_error_summary", "app/scheduler", "scheduler dispatch batch error summary",
		logging.Field{Key: "total_errors", Value: totalErrors},
		logging.Field{Key: "unique_causes", Value: len(ordered)},
		logging.Field{Key: "top_n", Value: limit},
	)
	for idx := 0; idx < limit; idx++ {
		entry := ordered[idx]
		category, cause := parseErrorCauseKey(entry.cause)
		s.logger.Info(context.Background(), "app.scheduler.dispatch_batch_error_cause", "app/scheduler", "scheduler dispatch batch error cause",
			logging.Field{Key: "rank", Value: idx + 1},
			logging.Field{Key: "category", Value: category},
			logging.Field{Key: "cause", Value: cause},
			logging.Field{Key: "count", Value: entry.count},
			logging.Field{Key: "sample_article_ids", Value: entry.sampleIDs},
		)
	}
}

// normalizeBatchErrorCause produces a stable grouping key from a raw batch item error text.
// The rawError parameter is the direct item.Error message from extraction batch persistence output.
// It returns a normalized cause string that trims whitespace and keeps the prefix before the first colon.
func normalizeBatchErrorCause(rawError string) string {
	trimmed := strings.TrimSpace(rawError)
	if trimmed == "" {
		return "unknown error"
	}
	if isRateLimitedError(trimmed) {
		switch {
		case strings.Contains(strings.ToLower(trimmed), "tpm"),
			strings.Contains(strings.ToLower(trimmed), "tokens per min"),
			strings.Contains(strings.ToLower(trimmed), "tokens per minute"):
			return "rate_limit_tpm"
		default:
			return "rate_limit"
		}
	}

	prefix, _, found := strings.Cut(trimmed, ":")
	if !found {
		return trimmed
	}

	normalized := strings.TrimSpace(prefix)
	if normalized == "" {
		return trimmed
	}
	return normalized
}

// classifyError classifies a raw extraction error text into a stable operational category.
// The errText parameter is the full wrapped error chain text emitted by extraction orchestration.
// It returns one of the known categories for log analytics and falls back to "unknown".
func classifyError(errText string) string {
	normalized := strings.ToLower(strings.TrimSpace(errText))
	switch {
	case isRateLimitedError(normalized):
		return "llm_rate_limit"
	case strings.Contains(normalized, "non-2xx response"):
		return "llm_http_non_2xx"
	case strings.Contains(normalized, "http failure"):
		return "llm_http_failure"
	case strings.Contains(normalized, "json decode failure"):
		return "llm_json_decode"
	case strings.Contains(normalized, "validate extract result:"):
		return "extract_validation"
	case strings.Contains(normalized, "convert done record:"),
		strings.Contains(normalized, "convert article_id="):
		return "article_conversion"
	case strings.Contains(normalized, "persist failure state:"):
		return "persist_failure"
	default:
		return "unknown"
	}
}

func sanitizeErrorText(errText string) string {
	trimmed := strings.TrimSpace(errText)
	if trimmed == "" {
		return ""
	}
	lowered := strings.ToLower(trimmed)
	bodyIdx := strings.Index(lowered, " body=")
	if bodyIdx >= 0 {
		return strings.TrimSpace(trimmed[:bodyIdx]) + " body=[redacted]"
	}
	return trimmed
}

// buildErrorCauseKey creates one deterministic map key from category and cause.
// The category parameter is a classifyError result and cause is a normalized cause text.
// It returns a parseable key used for grouped counting and ranking.
func buildErrorCauseKey(category string, cause string) string {
	return category + "|" + cause
}

// parseErrorCauseKey splits one grouped error key into category and cause values.
// The key parameter must be the buildErrorCauseKey output format.
// It returns parsed category/cause, or falls back to unknown + raw key for compatibility.
func parseErrorCauseKey(key string) (string, string) {
	category, cause, ok := strings.Cut(key, "|")
	if !ok || strings.TrimSpace(category) == "" || strings.TrimSpace(cause) == "" {
		return "unknown", key
	}
	return category, cause
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `{"error":"sanitize_failed"}`
	}
	return string(encoded)
}

// chunkIDs splits IDs into fixed-size chunks preserving original order.
// The ids parameter is the full ordered set, and batchSize controls max chunk length.
// It returns a slice of non-empty chunks.
func chunkIDs(ids []int64, batchSize int) [][]int64 {
	if len(ids) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(ids)
	}
	chunks := make([][]int64, 0, (len(ids)+batchSize-1)/batchSize)
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[start:end])
	}
	return chunks
}
