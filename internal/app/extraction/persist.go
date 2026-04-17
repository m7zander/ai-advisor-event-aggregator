// Package extraction provides app-layer extraction orchestration.
// This file integrates extraction lifecycle persistence and idempotent execution guards.
package extraction

import (
	"context"
	"fmt"
	"time"

	"ai-advisor-impact-service/internal/extract"
	repopkg "ai-advisor-impact-service/internal/repository/extraction"
)

const (
	// ExecutionOutcomeNewlyExtracted indicates LLM was called and a fresh result was persisted.
	ExecutionOutcomeNewlyExtracted ExecutionOutcome = "newly_extracted"
	// ExecutionOutcomeAlreadyDone indicates existing done result was reused without LLM call.
	ExecutionOutcomeAlreadyDone ExecutionOutcome = "already_done"
	// ExecutionOutcomeAlreadyPending indicates another caller is already processing extraction.
	ExecutionOutcomeAlreadyPending ExecutionOutcome = "already_pending"
	// ExecutionOutcomeAlreadyFailed indicates a previous failure exists and no retry was attempted.
	ExecutionOutcomeAlreadyFailed ExecutionOutcome = "already_failed"
)

// ExecutionOutcome describes idempotent extraction execution result classification.
type ExecutionOutcome string

// StateRepository defines persistence operations for extraction lifecycle status and results.
type StateRepository interface {
	ClaimPending(ctx context.Context, articleID int64, model string, startedAt time.Time) (repopkg.ClaimResult, error)
	UpsertSuccess(ctx context.Context, articleID int64, model string, finishedAt time.Time, result extract.ExtractResult) error
	UpsertFailure(ctx context.Context, articleID int64, model string, finishedAt time.Time, errMsg string) error
}

// RunPersistOutcome contains explicit idempotent execution state and optional extraction payload.
type RunPersistOutcome struct {
	Outcome ExecutionOutcome
	Result  *extract.ExtractResult
	Error   string
}

// RunAndPersistWithOutcome executes single-article extraction with atomic pending claim and persistence transitions.
// Parameters: ctx controls lifecycle, article is app-layer input, extractor performs LLM extraction,
// repo provides claim/state persistence, and model identifies extractor metadata.
// It returns explicit outcome semantics or an error for infrastructure failures.
func RunAndPersistWithOutcome(ctx context.Context, article Article, extractor Extractor, repo StateRepository, model string) (RunPersistOutcome, error) {
	if repo == nil {
		return RunPersistOutcome{}, fmt.Errorf("state repository is nil")
	}
	if extractor == nil {
		return RunPersistOutcome{}, fmt.Errorf("extractor is nil")
	}
	if model == "" {
		return RunPersistOutcome{}, fmt.Errorf("model must not be empty")
	}

	claim, err := repo.ClaimPending(ctx, article.ID, model, time.Now().UTC())
	if err != nil {
		return RunPersistOutcome{}, fmt.Errorf("claim extraction: %w", err)
	}

	switch claim.Outcome {
	case repopkg.ClaimOutcomeAlreadyDone:
		result, convErr := recordToExtractResult(claim.Existing)
		if convErr != nil {
			return RunPersistOutcome{}, fmt.Errorf("convert done record: %w", convErr)
		}
		return RunPersistOutcome{Outcome: ExecutionOutcomeAlreadyDone, Result: &result}, nil
	case repopkg.ClaimOutcomeAlreadyPending:
		return RunPersistOutcome{Outcome: ExecutionOutcomeAlreadyPending}, nil
	case repopkg.ClaimOutcomeAlreadyFailed:
		failureMsg := "previous extraction failed"
		if claim.Existing != nil && claim.Existing.ExtractionError != nil {
			failureMsg = *claim.Existing.ExtractionError
		}
		return RunPersistOutcome{Outcome: ExecutionOutcomeAlreadyFailed, Error: failureMsg}, nil
	case repopkg.ClaimOutcomeGranted:
		// proceed below
	default:
		return RunPersistOutcome{}, fmt.Errorf("unexpected claim outcome %q", claim.Outcome)
	}

	out, err := Run(ctx, article, extractor)
	if err != nil {
		finishedAt := time.Now().UTC()
		if persistErr := repo.UpsertFailure(ctx, article.ID, model, finishedAt, err.Error()); persistErr != nil {
			return RunPersistOutcome{}, fmt.Errorf("persist failure state: %w", persistErr)
		}
		return RunPersistOutcome{}, fmt.Errorf("run extraction: %w", err)
	}

	finishedAt := time.Now().UTC()
	if err := repo.UpsertSuccess(ctx, article.ID, model, finishedAt, out); err != nil {
		return RunPersistOutcome{}, fmt.Errorf("persist success state: %w", err)
	}

	return RunPersistOutcome{Outcome: ExecutionOutcomeNewlyExtracted, Result: &out}, nil
}

// RunAndPersist executes single-article extraction and returns the extracted result.
// It wraps RunAndPersistWithOutcome and is kept for compatibility with existing callers.
// It returns a result for newly extracted/already done outcomes, or an error otherwise.
func RunAndPersist(ctx context.Context, article Article, extractor Extractor, repo StateRepository, model string) (extract.ExtractResult, error) {
	outcome, err := RunAndPersistWithOutcome(ctx, article, extractor, repo, model)
	if err != nil {
		return extract.ExtractResult{}, err
	}
	if outcome.Result == nil {
		return extract.ExtractResult{}, fmt.Errorf("extraction not executed: %s", outcome.Outcome)
	}
	return *outcome.Result, nil
}

// recordToExtractResult converts a persisted done record into extract result contract.
// The rec parameter must represent a completed extraction with populated extracted fields.
// It returns a validated extract result or an error when required fields are missing/invalid.
func recordToExtractResult(rec *repopkg.Record) (extract.ExtractResult, error) {
	if rec == nil {
		return extract.ExtractResult{}, fmt.Errorf("record is nil")
	}
	if rec.ExtractedEventType == nil || rec.ExtractedGeoCluster == nil || rec.ExtractedImpactDirection == nil ||
		rec.ExtractedImpactStrength == nil || rec.ExtractedTimeHorizon == nil || rec.ExtractedConfidence == nil {
		return extract.ExtractResult{}, fmt.Errorf("record missing required extracted fields")
	}
	result := extract.ExtractResult{
		ArticleID:       rec.ArticleID,
		EventType:       *rec.ExtractedEventType,
		GeoCluster:      *rec.ExtractedGeoCluster,
		Countries:       rec.ExtractedCountries,
		Companies:       rec.ExtractedCompanies,
		Sectors:         rec.ExtractedSectors,
		Industries:      rec.ExtractedIndustries,
		ImpactDirection: *rec.ExtractedImpactDirection,
		ImpactStrength:  *rec.ExtractedImpactStrength,
		Channels:        rec.ExtractedChannels,
		TimeHorizon:     *rec.ExtractedTimeHorizon,
		Confidence:      *rec.ExtractedConfidence,
	}
	if err := result.Validate(); err != nil {
		return extract.ExtractResult{}, fmt.Errorf("validate converted result: %w", err)
	}
	return result, nil
}
