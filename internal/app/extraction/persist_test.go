// Package extraction tests persistence-integrated single-article orchestration behavior.
package extraction

import (
	"context"
	"errors"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/extract"
	repopkg "ai-advisor-impact-service/internal/repository/extraction"
)

// fakeStateRepo captures persistence method calls for RunAndPersist tests.
type fakeStateRepo struct {
	pendingCalls int
	successCalls int
	failureCalls int
	lastErrorMsg string
	claimResult  repopkg.ClaimResult
}

// ClaimPending returns configurable claim outcomes and tracks claim invocations.
// Parameters are accepted to satisfy StateRepository interface.
// It returns configured claim result or granted by default.
func (f *fakeStateRepo) ClaimPending(_ context.Context, articleID int64, model string, startedAt time.Time) (repopkg.ClaimResult, error) {
	_ = articleID
	_ = model
	_ = startedAt
	f.pendingCalls++
	if f.claimResult.Outcome == "" {
		return repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeGranted}, nil
	}
	return f.claimResult, nil
}

// UpsertSuccess records success-state persistence calls.
// The parameters are accepted to satisfy the StateRepository interface.
// It returns nil to emulate successful persistence.
func (f *fakeStateRepo) UpsertSuccess(_ context.Context, _ int64, _ string, _ time.Time, _ extract.ExtractResult) error {
	f.successCalls++
	return nil
}

// UpsertFailure records failure-state persistence calls and captured message.
// The errMsg parameter is stored for assertions.
// It returns nil to emulate successful persistence.
func (f *fakeStateRepo) UpsertFailure(_ context.Context, _ int64, _ string, _ time.Time, errMsg string) error {
	f.failureCalls++
	f.lastErrorMsg = errMsg
	return nil
}

// persistFakeExtractor is a minimal fake extractor for RunAndPersist tests.
type persistFakeExtractor struct {
	out   extract.ExtractResult
	err   error
	calls int
}

// Extract returns configured result/error for persistence flow tests.
// The context and input parameters satisfy interface requirements.
// It returns deterministic preconfigured values.
func (f *persistFakeExtractor) Extract(_ context.Context, _ extract.ExtractInput) (extract.ExtractResult, error) {
	f.calls++
	if f.err != nil {
		return extract.ExtractResult{}, f.err
	}
	return f.out, nil
}

// TestRunAndPersist_PendingThenDone verifies success flow persists pending then done states.
// It executes RunAndPersist with a successful fake extractor.
// It fails if expected persistence transitions are missing.
func TestRunAndPersist_PendingThenDone(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{
		ID:          501,
		Title:       "Title",
		Link:        "https://example.com",
		Source:      "source",
		PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC),
		Content:     &content,
	}
	repo := &fakeStateRepo{}
	extractor := &persistFakeExtractor{out: extract.ExtractResult{
		ArticleID:       501,
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
	}}

	outcome, err := RunAndPersistWithOutcome(context.Background(), article, extractor, repo, "gpt-4o-mini")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if outcome.Outcome != ExecutionOutcomeNewlyExtracted {
		t.Fatalf("expected newly_extracted outcome, got %s", outcome.Outcome)
	}
	if repo.pendingCalls != 1 || repo.successCalls != 1 || repo.failureCalls != 0 {
		t.Fatalf("unexpected persistence calls: %+v", repo)
	}
}

// TestRunAndPersist_PendingThenFailed verifies failure flow persists pending then failed states.
// It executes RunAndPersist with a failing fake extractor.
// It fails if failure state is not persisted.
func TestRunAndPersist_PendingThenFailed(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{
		ID:          502,
		Title:       "Title",
		Link:        "https://example.com",
		Source:      "source",
		PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC),
		Content:     &content,
	}
	repo := &fakeStateRepo{}
	extractor := &persistFakeExtractor{err: errors.New("extractor boom")}

	if _, err := RunAndPersistWithOutcome(context.Background(), article, extractor, repo, "gpt-4o-mini"); err == nil {
		t.Fatal("expected extraction error")
	}
	if repo.pendingCalls != 1 || repo.successCalls != 0 || repo.failureCalls != 1 {
		t.Fatalf("unexpected persistence calls: %+v", repo)
	}
	if repo.lastErrorMsg == "" {
		t.Fatal("expected persisted failure message")
	}
}

// TestRunAndPersistWithOutcome_AlreadyDone verifies done records are returned without extra LLM call.
// It configures a claim result indicating already_done with existing extracted record.
// It fails if extractor is called or done result is not reused.
func TestRunAndPersistWithOutcome_AlreadyDone(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{ID: 503, Title: "Title", Link: "https://example.com", Source: "source", PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), Content: &content}
	eventType := extract.EventTypeMacro
	geo := extract.GeoClusterGlobal
	direction := extract.ImpactDirectionNeutral
	strength := 10
	horizon := extract.TimeHorizonShort
	confidence := 0.7
	repo := &fakeStateRepo{claimResult: repopkg.ClaimResult{
		Outcome: repopkg.ClaimOutcomeAlreadyDone,
		Existing: &repopkg.Record{
			ArticleID:                503,
			ExtractionStatus:         repopkg.StatusDone,
			ExtractedEventType:       &eventType,
			ExtractedGeoCluster:      &geo,
			ExtractedCountries:       []string{"US"},
			ExtractedCompanies:       []string{"ACME"},
			ExtractedSectors:         []string{"industrials"},
			ExtractedIndustries:      []string{"Software - Application"},
			ExtractedImpactDirection: &direction,
			ExtractedImpactStrength:  &strength,
			ExtractedChannels:        []extract.Channel{extract.ChannelRiskSentiment},
			ExtractedTimeHorizon:     &horizon,
			ExtractedConfidence:      &confidence,
		},
	}}
	extractor := &persistFakeExtractor{out: extract.ExtractResult{ArticleID: 503}}

	outcome, err := RunAndPersistWithOutcome(context.Background(), article, extractor, repo, "gpt-4o-mini")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if outcome.Outcome != ExecutionOutcomeAlreadyDone || outcome.Result == nil {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if extractor.calls != 0 {
		t.Fatal("expected no LLM calls for already_done state")
	}
}

// TestRunAndPersistWithOutcome_AlreadyPending verifies pending state denies duplicate LLM calls.
// It configures a claim result indicating already_pending.
// It fails if extractor is called or wrong outcome returned.
func TestRunAndPersistWithOutcome_AlreadyPending(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{ID: 504, Title: "Title", Link: "https://example.com", Source: "source", PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), Content: &content}
	repo := &fakeStateRepo{claimResult: repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyPending, Existing: &repopkg.Record{ArticleID: 504, ExtractionStatus: repopkg.StatusPending}}}
	extractor := &persistFakeExtractor{}

	outcome, err := RunAndPersistWithOutcome(context.Background(), article, extractor, repo, "gpt-4o-mini")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if outcome.Outcome != ExecutionOutcomeAlreadyPending {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if extractor.calls != 0 {
		t.Fatal("expected no LLM calls for already_pending state")
	}
}

// TestRunAndPersistWithOutcome_AlreadyFailed verifies failed state does not retry LLM by default.
// It configures a claim result indicating already_failed.
// It fails if extractor is called or wrong outcome returned.
func TestRunAndPersistWithOutcome_AlreadyFailed(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{ID: 505, Title: "Title", Link: "https://example.com", Source: "source", PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), Content: &content}
	msg := "previous failure"
	repo := &fakeStateRepo{claimResult: repopkg.ClaimResult{Outcome: repopkg.ClaimOutcomeAlreadyFailed, Existing: &repopkg.Record{ArticleID: 505, ExtractionStatus: repopkg.StatusFailed, ExtractionError: &msg}}}
	extractor := &persistFakeExtractor{}

	outcome, err := RunAndPersistWithOutcome(context.Background(), article, extractor, repo, "gpt-4o-mini")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if outcome.Outcome != ExecutionOutcomeAlreadyFailed || outcome.Error == "" {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if extractor.calls != 0 {
		t.Fatal("expected no LLM calls for already_failed state")
	}
}

// TestRunAndPersistWithOutcome_NilExtractorDoesNotClaim verifies infrastructure misconfiguration fails before claiming state.
// It calls RunAndPersistWithOutcome with nil extractor and a valid repo/model.
// It fails if claim is attempted for nil extractor scenarios.
func TestRunAndPersistWithOutcome_NilExtractorDoesNotClaim(t *testing.T) {
	content := "Sufficient content for preprocessing to produce extractor input text."
	article := Article{ID: 506, Title: "Title", Link: "https://example.com", Source: "source", PublishedAt: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), Content: &content}
	repo := &fakeStateRepo{}

	if _, err := RunAndPersistWithOutcome(context.Background(), article, nil, repo, "gpt-4o-mini"); err == nil {
		t.Fatal("expected nil extractor error")
	}
	if repo.pendingCalls != 0 {
		t.Fatalf("expected no claim attempt when extractor is nil, got %d", repo.pendingCalls)
	}
}
