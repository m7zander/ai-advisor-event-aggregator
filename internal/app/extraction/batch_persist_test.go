// Package extraction tests persistence-guarded batch orchestration behavior.
package extraction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
)

// inMemoryClaimRepo is a deterministic in-memory StateRepository for idempotent batch tests.
type inMemoryClaimRepo struct {
	mu      sync.Mutex
	records map[int64]repopkg.Record
}

// ClaimPending atomically grants first pending claim for article and denies subsequent claims by existing status.
// Parameters identify article/model and claim timestamp.
// It returns claim outcome and existing record when denied.
func (r *inMemoryClaimRepo) ClaimPending(_ context.Context, articleID int64, model string, startedAt time.Time) (repopkg.ClaimResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.records == nil {
		r.records = map[int64]repopkg.Record{}
	}
	rec, ok := r.records[articleID]
	if !ok {
		rec = repopkg.Record{ArticleID: articleID, ExtractionStatus: repopkg.StatusPending, ExtractionModel: model, ExtractionStartedAt: &startedAt}
		r.records[articleID] = rec
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeGranted}, nil
	}
	switch rec.ExtractionStatus {
	case repopkg.StatusDone:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyDone, Existing: &rec}, nil
	case repopkg.StatusPending:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyPending, Existing: &rec}, nil
	case repopkg.StatusFailed:
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyFailed, Existing: &rec}, nil
	default:
		return repopkg.ClaimResult{}, fmt.Errorf("unknown status")
	}
}

// UpsertSuccess writes done status and extracted fields.
// Parameters identify article/model and extraction result payload.
// It returns nil when in-memory update succeeds.
func (r *inMemoryClaimRepo) UpsertSuccess(_ context.Context, articleID int64, model string, _ time.Time, result extract.ExtractResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[articleID]
	rec.ArticleID = articleID
	rec.ExtractionStatus = repopkg.StatusDone
	rec.ExtractionModel = model
	eventType := result.EventType
	geo := result.GeoCluster
	direction := result.ImpactDirection
	strength := result.ImpactStrength
	horizon := result.TimeHorizon
	confidence := result.Confidence
	rec.ExtractedEventType = &eventType
	rec.ExtractedGeoCluster = &geo
	rec.ExtractedCountries = result.Countries
	rec.ExtractedCompanies = result.Companies
	rec.ExtractedSectors = result.Sectors
	rec.ExtractedIndustries = result.Industries
	rec.ExtractedImpactDirection = &direction
	rec.ExtractedImpactStrength = &strength
	rec.ExtractedChannels = result.Channels
	rec.ExtractedTimeHorizon = &horizon
	rec.ExtractedConfidence = &confidence
	r.records[articleID] = rec
	return nil
}

// UpsertFailure writes failed status and error message.
// Parameters identify article/model and failure message.
// It returns nil when in-memory update succeeds.
func (r *inMemoryClaimRepo) UpsertFailure(_ context.Context, articleID int64, model string, _ time.Time, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[articleID]
	rec.ArticleID = articleID
	rec.ExtractionStatus = repopkg.StatusFailed
	rec.ExtractionModel = model
	rec.ExtractionError = &errMsg
	r.records[articleID] = rec
	return nil
}

// countingExtractor tracks number of extraction calls per article and returns valid results.
type countingExtractor struct {
	calls sync.Map
}

// Extract increments per-article call counter and returns a valid extraction result.
// The ctx parameter is unused and accepted for interface compliance.
// It returns a deterministic valid result.
func (e *countingExtractor) Extract(_ context.Context, in extract.ExtractInput) (extract.ExtractResult, error) {
	counterAny, _ := e.calls.LoadOrStore(in.ArticleID, new(int32))
	counter := counterAny.(*int32)
	atomic.AddInt32(counter, 1)
	return extract.ExtractResult{
		ArticleID:       in.ArticleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		Industries:      []string{"Software - Application"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  20,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.7,
	}, nil
}

// callCount returns tracked call count for one article id.
// The articleID parameter identifies the tracked call counter.
// It returns zero when article was never extracted.
func (e *countingExtractor) callCount(articleID int64) int32 {
	if counterAny, ok := e.calls.Load(articleID); ok {
		return atomic.LoadInt32(counterAny.(*int32))
	}
	return 0
}

// TestRunBatchAndPersistWithOutcome_DuplicateArticleIDsSingleLLMCall verifies duplicate IDs in one batch trigger at most one LLM call.
// It runs two identical article IDs in the same batch with high concurrency.
// It fails if extraction call count exceeds one for the duplicate article id.
func TestRunBatchAndPersistWithOutcome_DuplicateArticleIDsSingleLLMCall(t *testing.T) {
	content := "This article contains enough meaningful words for preprocessing to keep content."
	articles := []Article{
		{ID: 701, Title: "t", Link: "https://x", Source: "s", PublishedAt: time.Date(2026, 3, 28, 11, 0, 0, 0, time.UTC), Content: &content},
		{ID: 701, Title: "t", Link: "https://x", Source: "s", PublishedAt: time.Date(2026, 3, 28, 11, 0, 0, 0, time.UTC), Content: &content},
	}
	repo := &inMemoryClaimRepo{}
	extractor := &countingExtractor{}

	res, err := RunBatchAndPersistWithOutcome(context.Background(), articles, extractor, repo, "gpt-4o-mini", BatchOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("unexpected item count: %d", len(res.Items))
	}
	if extractor.callCount(701) != 1 {
		t.Fatalf("expected exactly one LLM call for duplicate article id, got %d", extractor.callCount(701))
	}
}
