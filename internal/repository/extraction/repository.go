// Package extractionrepo provides persistence for per-article extraction status and results.
// It stores extraction lifecycle state and validated extraction output keyed by article_id.
package extractionrepo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/observability"
)

const extractionSchemaSetup = `
CREATE TABLE IF NOT EXISTS impact_service_article_extractions (
    article_id BIGINT PRIMARY KEY,
    extraction_status TEXT NOT NULL,
    extraction_model TEXT NOT NULL,
    extraction_started_at TIMESTAMP NULL,
    extraction_finished_at TIMESTAMP NULL,
    extraction_error TEXT NULL,
    extracted_event_type TEXT NULL,
    extracted_geo_cluster TEXT NULL,
    extracted_countries TEXT NULL,
    extracted_companies TEXT NULL,
    extracted_sectors TEXT NULL,
    extracted_industries TEXT NULL,
    extracted_impact_direction TEXT NULL,
    extracted_impact_strength INTEGER NULL,
    extracted_channels TEXT NULL,
    extracted_time_horizon TEXT NULL,
    extracted_confidence DOUBLE PRECISION NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_impact_service_article_extractions_article_id ON impact_service_article_extractions(article_id);
CREATE INDEX IF NOT EXISTS idx_impact_service_article_extractions_status ON impact_service_article_extractions(extraction_status);
CREATE TABLE IF NOT EXISTS impact_service_aggregated_events (
    event_id TEXT PRIMARY KEY,
    cluster_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    geo_cluster TEXT NOT NULL,
    countries TEXT NOT NULL,
    sectors TEXT NOT NULL,
    industries TEXT NOT NULL DEFAULT '[]',
    direction TEXT NOT NULL,
    strength INTEGER NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    article_ids TEXT NOT NULL,
    source_count INTEGER NOT NULL,
    first_seen_at TIMESTAMP NOT NULL,
    last_seen_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_impact_service_aggregated_events_last_seen_at ON impact_service_aggregated_events(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_impact_service_aggregated_events_cluster_key ON impact_service_aggregated_events(cluster_key);
CREATE TABLE IF NOT EXISTS impact_service_clustering_state (
    state_key TEXT PRIMARY KEY,
    last_run_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_impact_service_aggregated_events_cluster_key_unique ON impact_service_aggregated_events(cluster_key);`

const extractionLegacyRenameSQL = `
ALTER TABLE IF EXISTS article_extractions RENAME TO impact_service_article_extractions;
ALTER TABLE IF EXISTS aggregated_events RENAME TO impact_service_aggregated_events;
ALTER TABLE IF EXISTS clustering_state RENAME TO impact_service_clustering_state;
DO $$
BEGIN
    IF to_regclass('idx_article_extractions_article_id') IS NOT NULL
       AND to_regclass('idx_impact_service_article_extractions_article_id') IS NULL THEN
        ALTER INDEX idx_article_extractions_article_id RENAME TO idx_impact_service_article_extractions_article_id;
    END IF;
    IF to_regclass('idx_article_extractions_status') IS NOT NULL
       AND to_regclass('idx_impact_service_article_extractions_status') IS NULL THEN
        ALTER INDEX idx_article_extractions_status RENAME TO idx_impact_service_article_extractions_status;
    END IF;
    IF to_regclass('idx_aggregated_events_last_seen_at') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_last_seen_at') IS NULL THEN
        ALTER INDEX idx_aggregated_events_last_seen_at RENAME TO idx_impact_service_aggregated_events_last_seen_at;
    END IF;
    IF to_regclass('idx_aggregated_events_cluster_key') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_cluster_key') IS NULL THEN
        ALTER INDEX idx_aggregated_events_cluster_key RENAME TO idx_impact_service_aggregated_events_cluster_key;
    END IF;
    IF to_regclass('idx_aggregated_events_cluster_key_unique') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_cluster_key_unique') IS NULL THEN
        ALTER INDEX idx_aggregated_events_cluster_key_unique RENAME TO idx_impact_service_aggregated_events_cluster_key_unique;
    END IF;
END $$;`

const (
	// StatusPending marks extraction as started and in progress.
	StatusPending = "pending"
	// StatusDone marks extraction as successfully completed.
	StatusDone = "done"
	// StatusFailed marks extraction as failed.
	StatusFailed = "failed"
)

const (
	// ClaimOutcomeGranted indicates caller claimed pending execution and may run LLM extraction.
	ClaimOutcomeGranted ClaimOutcome = "granted"
	// ClaimOutcomeAlreadyDone indicates extraction was already completed previously.
	ClaimOutcomeAlreadyDone ClaimOutcome = "already_done"
	// ClaimOutcomeAlreadyPending indicates extraction is already in progress.
	ClaimOutcomeAlreadyPending ClaimOutcome = "already_pending"
	// ClaimOutcomeAlreadyFailed indicates extraction previously failed and should not retry by default.
	ClaimOutcomeAlreadyFailed ClaimOutcome = "already_failed"
)

// ClaimOutcome describes the result of an atomic pending-claim attempt.
type ClaimOutcome string

// ClaimResult returns claim outcome details and existing record when claim is denied.
type ClaimResult struct {
	Outcome  ClaimOutcome
	Existing *Record
}

// Repository persists extraction lifecycle and result data.
type Repository struct {
	db *sql.DB
}

// Record represents one stored extraction row.
type Record struct {
	ArticleID                int64
	ExtractionStatus         string
	ExtractionModel          string
	ExtractionStartedAt      *time.Time
	ExtractionFinishedAt     *time.Time
	ExtractionError          *string
	ExtractedEventType       *extract.EventType
	ExtractedGeoCluster      *extract.GeoCluster
	ExtractedCountries       []string
	ExtractedCompanies       []string
	ExtractedSectors         []string
	ExtractedIndustries      []string
	ExtractedImpactDirection *extract.ImpactDirection
	ExtractedImpactStrength  *int
	ExtractedChannels        []extract.Channel
	ExtractedTimeHorizon     *extract.TimeHorizon
	ExtractedConfidence      *float64
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// NewRepository constructs an extraction repository bound to a SQL database handle.
// The db parameter is the initialized SQL DB connection pool.
// It returns a repository instance or an error when db is nil.
func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	return &Repository{db: db}, nil
}

// Migrate creates extraction persistence schema if it does not yet exist.
// The ctx parameter controls statement execution lifecycle.
// It returns an error when schema creation fails.
func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.db.ExecContext(ctx, extractionLegacyRenameSQL); err != nil {
		return fmt.Errorf("rename legacy extraction tables: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, extractionSchemaSetup); err != nil {
		return fmt.Errorf("migrate extraction schema: %w", err)
	}

	if err := r.applyLegacySchemaFixups(ctx); err != nil {
		return fmt.Errorf("migrate extraction schema legacy fixups failed: %w", err)
	}
	return nil
}

// applyLegacySchemaFixups upgrades already-existing legacy tables by adding newly introduced columns.
// The ctx parameter controls statement execution and metadata-query lifecycle.
// It returns an error when legacy table detection or fix-up statements fail.
func (r *Repository) applyLegacySchemaFixups(ctx context.Context) error {
	legacyFixups := []struct {
		tableName string
		statement string
	}{
		{
			tableName: "impact_service_aggregated_events",
			statement: `ALTER TABLE impact_service_aggregated_events ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
		},
		{
			tableName: "impact_service_aggregated_events",
			statement: `ALTER TABLE impact_service_aggregated_events ADD COLUMN IF NOT EXISTS industries TEXT NOT NULL DEFAULT '[]'`,
		},
		{
			tableName: "impact_service_article_extractions",
			statement: `ALTER TABLE impact_service_article_extractions ADD COLUMN IF NOT EXISTS extracted_industries TEXT NULL`,
		},
	}
	for _, fixup := range legacyFixups {
		exists, err := r.tableExists(ctx, fixup.tableName)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if _, err := r.db.ExecContext(ctx, fixup.statement); err != nil && !isIgnorableMigrationError(err) {
			return err
		}
	}
	return nil
}

// tableExists checks if a table exists in the current schema search path.
// The ctx parameter controls metadata query lifetime and tableName identifies the table to inspect.
// It returns true when the table is present and false when no matching table is found.
func (r *Repository) tableExists(ctx context.Context, tableName string) (bool, error) {
	var exists int
	err := r.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`,
		tableName,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check table exists %s: %w", tableName, err)
	}
	return true, nil
}

// isIgnorableMigrationError returns whether a migration error can be safely ignored for idempotent schema evolution.
// The err parameter is the SQL execution error returned from migration fix-up statements.
// It returns true for known "already exists" class errors across PostgreSQL drivers.
func isIgnorableMigrationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate column")
}

// ClaimPending atomically claims extraction execution for one article by inserting pending when absent.
// Parameters: articleID identifies article row, model stores extractor model metadata, startedAt marks claim time.
// It returns granted outcome for successful claimant, or existing-state outcome without re-claiming when row exists.
func (r *Repository) ClaimPending(ctx context.Context, articleID int64, model string, startedAt time.Time) (ClaimResult, error) {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.claim_pending")
	defer span.End()

	if articleID <= 0 {
		err := fmt.Errorf("article_id must be > 0")
		observability.RecordError(span, err)
		return ClaimResult{}, err
	}
	if model == "" {
		return ClaimResult{}, fmt.Errorf("model must not be empty")
	}

	const q = `
INSERT INTO impact_service_article_extractions (
  article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at, extraction_error,
  extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies, extracted_sectors,
  extracted_industries, extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
  extracted_confidence, created_at, updated_at
) VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $5, $6)
ON CONFLICT(article_id) DO NOTHING`

	now := startedAt.UTC()
	res, err := r.db.ExecContext(ctx, q, articleID, StatusPending, model, now, now, now)
	if err != nil {
		observability.RecordError(span, err)
		return ClaimResult{}, fmt.Errorf("claim pending: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		observability.RecordError(span, err)
		return ClaimResult{}, fmt.Errorf("claim pending rows affected: %w", err)
	}
	if affected == 1 {
		return ClaimResult{Outcome: ClaimOutcomeGranted}, nil
	}

	rec, err := r.GetByArticleID(ctx, articleID)
	if err != nil {
		observability.RecordError(span, err)
		return ClaimResult{}, fmt.Errorf("claim pending read existing: %w", err)
	}
	switch rec.ExtractionStatus {
	case StatusDone:
		return ClaimResult{Outcome: ClaimOutcomeAlreadyDone, Existing: &rec}, nil
	case StatusPending:
		return ClaimResult{Outcome: ClaimOutcomeAlreadyPending, Existing: &rec}, nil
	case StatusFailed:
		return ClaimResult{Outcome: ClaimOutcomeAlreadyFailed, Existing: &rec}, nil
	default:
		err := fmt.Errorf("claim pending unknown existing status %q", rec.ExtractionStatus)
		observability.RecordError(span, err)
		return ClaimResult{}, err
	}
}

// UpsertPending upserts pending extraction status for one article.
// Parameters: articleID identifies the article, model identifies the extractor model, startedAt marks lifecycle start.
// It returns an error when the upsert fails.
func (r *Repository) UpsertPending(ctx context.Context, articleID int64, model string, startedAt time.Time) error {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.upsert_pending")
	defer span.End()

	if articleID <= 0 {
		return fmt.Errorf("article_id must be > 0")
	}
	if model == "" {
		return fmt.Errorf("model must not be empty")
	}
	const q = `
INSERT INTO impact_service_article_extractions (
  article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at, extraction_error,
  extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies, extracted_sectors,
  extracted_industries, extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
  extracted_confidence, created_at, updated_at
) VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $5, $6)
ON CONFLICT(article_id) DO UPDATE SET
  extraction_status=excluded.extraction_status,
  extraction_model=excluded.extraction_model,
  extraction_started_at=excluded.extraction_started_at,
  extraction_finished_at=NULL,
  extraction_error=NULL,
  updated_at=excluded.updated_at`
	now := startedAt.UTC()
	if _, err := r.db.ExecContext(ctx, q, articleID, StatusPending, model, now, now, now); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("upsert pending: %w", err)
	}
	return nil
}

// UpsertSuccess upserts successful extraction status and validated extracted result.
// Parameters: articleID identifies the article, model identifies extractor model, finishedAt marks completion, result is validated extracted output.
// It returns an error when validation or persistence fails.
func (r *Repository) UpsertSuccess(ctx context.Context, articleID int64, model string, finishedAt time.Time, result extract.ExtractResult) error {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.upsert_success")
	defer span.End()

	if articleID <= 0 {
		return fmt.Errorf("article_id must be > 0")
	}
	if model == "" {
		return fmt.Errorf("model must not be empty")
	}
	if err := result.Validate(); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("validate result: %w", err)
	}
	if result.ArticleID != articleID {
		return fmt.Errorf("article_id mismatch: input=%d result=%d", articleID, result.ArticleID)
	}

	countriesJSON, err := json.Marshal(result.Countries)
	if err != nil {
		return fmt.Errorf("marshal countries: %w", err)
	}
	companiesJSON, err := json.Marshal(result.Companies)
	if err != nil {
		return fmt.Errorf("marshal companies: %w", err)
	}
	sectorsJSON, err := json.Marshal(result.Sectors)
	if err != nil {
		return fmt.Errorf("marshal sectors: %w", err)
	}
	industriesJSON, err := json.Marshal(result.Industries)
	if err != nil {
		return fmt.Errorf("marshal industries: %w", err)
	}
	channelsJSON, err := json.Marshal(result.Channels)
	if err != nil {
		return fmt.Errorf("marshal channels: %w", err)
	}

	const q = `
INSERT INTO impact_service_article_extractions (
  article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at, extraction_error,
  extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies, extracted_sectors, extracted_industries,
  extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
  extracted_confidence, created_at, updated_at
) VALUES ($1, $2, $3, NULL, $4, NULL, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT(article_id) DO UPDATE SET
  extraction_status=excluded.extraction_status,
  extraction_model=excluded.extraction_model,
  extraction_finished_at=excluded.extraction_finished_at,
  extraction_error=NULL,
  extracted_event_type=excluded.extracted_event_type,
  extracted_geo_cluster=excluded.extracted_geo_cluster,
  extracted_countries=excluded.extracted_countries,
  extracted_companies=excluded.extracted_companies,
  extracted_sectors=excluded.extracted_sectors,
  extracted_industries=excluded.extracted_industries,
  extracted_impact_direction=excluded.extracted_impact_direction,
  extracted_impact_strength=excluded.extracted_impact_strength,
  extracted_channels=excluded.extracted_channels,
  extracted_time_horizon=excluded.extracted_time_horizon,
  extracted_confidence=excluded.extracted_confidence,
  updated_at=excluded.updated_at`
	now := finishedAt.UTC()
	if _, err := r.db.ExecContext(ctx, q,
		articleID, StatusDone, model, now,
		result.EventType, result.GeoCluster, string(countriesJSON), string(companiesJSON), string(sectorsJSON), string(industriesJSON),
		result.ImpactDirection, result.ImpactStrength, string(channelsJSON), result.TimeHorizon,
		result.Confidence, now, now,
	); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("upsert success: %w", err)
	}
	return nil
}

// UpsertFailure upserts failed extraction status and stores the latest error message.
// Parameters: articleID identifies the article, model identifies extractor model, finishedAt marks completion, errMsg stores failure reason.
// It returns an error when persistence fails.
func (r *Repository) UpsertFailure(ctx context.Context, articleID int64, model string, finishedAt time.Time, errMsg string) error {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.upsert_failure")
	defer span.End()

	if articleID <= 0 {
		return fmt.Errorf("article_id must be > 0")
	}
	if model == "" {
		return fmt.Errorf("model must not be empty")
	}
	if errMsg == "" {
		return fmt.Errorf("error message must not be empty")
	}
	const q = `
INSERT INTO impact_service_article_extractions (
  article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at, extraction_error,
  extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies, extracted_sectors, extracted_industries,
  extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
  extracted_confidence, created_at, updated_at
) VALUES ($1, $2, $3, NULL, $4, $5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $6, $7)
ON CONFLICT(article_id) DO UPDATE SET
  extraction_status=excluded.extraction_status,
  extraction_model=excluded.extraction_model,
  extraction_finished_at=excluded.extraction_finished_at,
  extraction_error=excluded.extraction_error,
  updated_at=excluded.updated_at`
	now := finishedAt.UTC()
	if _, err := r.db.ExecContext(ctx, q, articleID, StatusFailed, model, now, errMsg, now, now); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("upsert failure: %w", err)
	}
	return nil
}

// GetByArticleID loads persisted extraction state by article_id.
// The articleID parameter identifies the target article record.
// It returns the record and a nil error, sql.ErrNoRows when absent, or another wrapped error.
func (r *Repository) GetByArticleID(ctx context.Context, articleID int64) (Record, error) {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.get_by_article_id")
	defer span.End()

	const q = `SELECT article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at,
extraction_error, extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies,
extracted_sectors, extracted_industries, extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
extracted_confidence, created_at, updated_at
FROM impact_service_article_extractions WHERE article_id = $1`

	var rec Record
	var startedAt, finishedAt sql.NullTime
	var extractionError sql.NullString
	var eventType, geoCluster sql.NullString
	var countries, companies, sectors, industries sql.NullString
	var impactDirection sql.NullString
	var impactStrength sql.NullInt64
	var channels sql.NullString
	var timeHorizon sql.NullString
	var confidence sql.NullFloat64

	err := r.db.QueryRowContext(ctx, q, articleID).Scan(
		&rec.ArticleID, &rec.ExtractionStatus, &rec.ExtractionModel, &startedAt, &finishedAt,
		&extractionError, &eventType, &geoCluster, &countries, &companies,
		&sectors, &industries, &impactDirection, &impactStrength, &channels, &timeHorizon,
		&confidence, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		observability.RecordError(span, err)
		return Record{}, fmt.Errorf("get by article_id: %w", err)
	}

	if startedAt.Valid {
		t := startedAt.Time
		rec.ExtractionStartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		rec.ExtractionFinishedAt = &t
	}
	if extractionError.Valid {
		s := extractionError.String
		rec.ExtractionError = &s
	}
	if eventType.Valid {
		v := extract.EventType(eventType.String)
		rec.ExtractedEventType = &v
	}
	if geoCluster.Valid {
		v := extract.GeoCluster(geoCluster.String)
		rec.ExtractedGeoCluster = &v
	}
	if impactDirection.Valid {
		v := extract.ImpactDirection(impactDirection.String)
		rec.ExtractedImpactDirection = &v
	}
	if impactStrength.Valid {
		v := int(impactStrength.Int64)
		rec.ExtractedImpactStrength = &v
	}
	if timeHorizon.Valid {
		v := extract.TimeHorizon(timeHorizon.String)
		rec.ExtractedTimeHorizon = &v
	}
	if confidence.Valid {
		v := confidence.Float64
		rec.ExtractedConfidence = &v
	}

	if countries.Valid {
		if err := json.Unmarshal([]byte(countries.String), &rec.ExtractedCountries); err != nil {
			observability.RecordError(span, err)
			return Record{}, fmt.Errorf("decode extracted_countries: %w", err)
		}
	}
	if companies.Valid {
		if err := json.Unmarshal([]byte(companies.String), &rec.ExtractedCompanies); err != nil {
			observability.RecordError(span, err)
			return Record{}, fmt.Errorf("decode extracted_companies: %w", err)
		}
	}
	if sectors.Valid {
		if err := json.Unmarshal([]byte(sectors.String), &rec.ExtractedSectors); err != nil {
			observability.RecordError(span, err)
			return Record{}, fmt.Errorf("decode extracted_sectors: %w", err)
		}
	}
	if industries.Valid {
		if err := json.Unmarshal([]byte(industries.String), &rec.ExtractedIndustries); err != nil {
			observability.RecordError(span, err)
			return Record{}, fmt.Errorf("decode extracted_industries: %w", err)
		}
	}
	if channels.Valid {
		if err := json.Unmarshal([]byte(channels.String), &rec.ExtractedChannels); err != nil {
			observability.RecordError(span, err)
			return Record{}, fmt.Errorf("decode extracted_channels: %w", err)
		}
	}
	if rec.ExtractedCountries == nil {
		rec.ExtractedCountries = []string{}
	}
	if rec.ExtractedCompanies == nil {
		rec.ExtractedCompanies = []string{}
	}
	if rec.ExtractedSectors == nil {
		rec.ExtractedSectors = []string{}
	}
	if rec.ExtractedIndustries == nil {
		rec.ExtractedIndustries = []string{}
	}
	if rec.ExtractedChannels == nil {
		rec.ExtractedChannels = []extract.Channel{}
	}

	return rec, nil
}
