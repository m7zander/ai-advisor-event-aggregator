// Package extractionrepo tests SQL persistence behavior for extraction lifecycle and result data.
package extractionrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	_ "github.com/lib/pq"
)

var extractionTestSchemaCounter uint64

// newTestRepository creates a PostgreSQL-backed repository in an isolated schema and applies schema migration.
// The t parameter controls test failure behavior.
// It returns the repository and cleanup function.
func newTestRepository(t *testing.T) (*Repository, func()) {
	t.Helper()
	db, cleanupDB := newPostgresTestDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		cleanupDB()
		t.Fatalf("new repo: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		cleanupDB()
		t.Fatalf("migrate: %v", err)
	}
	return repo, cleanupDB
}

// newPostgresTestDB opens a PostgreSQL test connection and configures a dedicated search_path schema.
// The t parameter controls failure handling and lifecycle cleanup.
// It returns a configured SQL connection and cleanup callback.
// It skips the test when TEST_POSTGRES_DSN is not set in the environment.
func newPostgresTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is required for PostgreSQL-backed repository tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("ping postgres: %v", err)
	}
	schema := fmt.Sprintf("extraction_test_%d_%d", time.Now().UnixNano(), atomic.AddUint64(&extractionTestSchemaCounter, 1))
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schema)); err != nil {
		_ = db.Close()
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf("SET search_path TO %s", schema)); err != nil {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		_ = db.Close()
		t.Fatalf("set search_path: %v", err)
	}
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		_ = db.Close()
	}
	return db, cleanup
}

// validResult returns a contract-valid extraction result for repository tests.
// The articleID parameter identifies the result article.
// It returns a valid extract.ExtractResult value.
func validResult(articleID int64) extract.ExtractResult {
	return extract.ExtractResult{
		ArticleID:       articleID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		Industries:      []string{"Software - Application"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  25,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.5,
	}
}

// TestRepository_Migrate_Idempotent verifies migration can run repeatedly without returning errors.
// It executes Migrate twice on the same PostgreSQL schema.
// It fails if repeated migration returns an error.
func TestRepository_Migrate_Idempotent(t *testing.T) {
	db, cleanup := newPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

// TestRepository_Migrate_RuntimeSchemaDoesNotReferenceLegacyImpactTables verifies runtime migration DDL excludes decommissioned impact tables.
// It inspects in-process migration SQL constants and fails when legacy impact-table names are present.
func TestRepository_Migrate_RuntimeSchemaDoesNotReferenceLegacyImpactTables(t *testing.T) {
	legacyTokens := []string{"event_security_impacts", "impact_service_event_security_impacts"}
	migrationSQL := []string{extractionLegacyRenameSQL, extractionSchemaSetup}

	for _, sqlText := range migrationSQL {
		for _, token := range legacyTokens {
			if strings.Contains(sqlText, token) {
				t.Fatalf("runtime migration SQL must not reference legacy impact tables: found %q", token)
			}
		}
	}
}

// TestRepository_Migrate_LegacyAggregatedEventsAddsIndustries verifies legacy impact_service_aggregated_events schemas are upgraded.
// It seeds an old table definition without industries and runs migration.
// It fails if migration does not add the industries column or if repeated migration errors.
func TestRepository_Migrate_LegacyAggregatedEventsAddsIndustries(t *testing.T) {
	db, cleanup := newPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	legacySchema := `
CREATE TABLE impact_service_aggregated_events (
    event_id TEXT PRIMARY KEY,
    cluster_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    geo_cluster TEXT NOT NULL,
    countries TEXT NOT NULL,
    sectors TEXT NOT NULL,
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
);`
	if _, err := db.ExecContext(context.Background(), legacySchema); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy schema: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy schema second run: %v", err)
	}

	if !postgresTableHasColumn(t, db, "impact_service_aggregated_events", "industries") {
		t.Fatal("expected industries column to be present after migration")
	}
}

// TestRepository_Migrate_LegacyArticleExtractionsAddsIndustries verifies legacy impact_service_article_extractions schemas are upgraded.
// It seeds an old table definition without extracted_industries and runs migration.
// It fails if migration does not add extracted_industries or if repeated migration errors.
func TestRepository_Migrate_LegacyArticleExtractionsAddsIndustries(t *testing.T) {
	db, cleanup := newPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	legacySchema := `
CREATE TABLE impact_service_article_extractions (
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
    extracted_impact_direction TEXT NULL,
    extracted_impact_strength INTEGER NULL,
    extracted_channels TEXT NULL,
    extracted_time_horizon TEXT NULL,
    extracted_confidence DOUBLE PRECISION NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);`
	if _, err := db.ExecContext(context.Background(), legacySchema); err != nil {
		t.Fatalf("seed legacy impact_service_article_extractions schema: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy impact_service_article_extractions schema: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy impact_service_article_extractions schema second run: %v", err)
	}

	if !postgresTableHasColumn(t, db, "impact_service_article_extractions", "extracted_industries") {
		t.Fatal("expected extracted_industries column to be present after migration")
	}
}

// TestRepository_Migrate_RenamesLegacyTablesToServicePrefix verifies one-time table rename upgrade path.
// It seeds legacy unprefixed extraction/event tables and validates migration renames them into canonical prefixed names.
func TestRepository_Migrate_RenamesLegacyTablesToServicePrefix(t *testing.T) {
	db, cleanup := newPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	legacySchema := `
CREATE TABLE article_extractions (
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
    extracted_impact_direction TEXT NULL,
    extracted_impact_strength INTEGER NULL,
    extracted_channels TEXT NULL,
    extracted_time_horizon TEXT NULL,
    extracted_confidence DOUBLE PRECISION NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE aggregated_events (
    event_id TEXT PRIMARY KEY,
    cluster_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    geo_cluster TEXT NOT NULL,
    countries TEXT NOT NULL,
    sectors TEXT NOT NULL,
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
CREATE TABLE clustering_state (
    state_key TEXT PRIMARY KEY,
    last_run_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);`
	if _, err := db.ExecContext(context.Background(), legacySchema); err != nil {
		t.Fatalf("seed legacy tables: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy table rename: %v", err)
	}

	if !postgresTableHasColumn(t, db, "impact_service_article_extractions", "article_id") {
		t.Fatal("expected canonical extraction table after migration")
	}
	if !postgresTableHasColumn(t, db, "impact_service_aggregated_events", "event_id") {
		t.Fatal("expected canonical aggregated events table after migration")
	}
	if !postgresTableHasColumn(t, db, "impact_service_clustering_state", "state_key") {
		t.Fatal("expected canonical clustering state table after migration")
	}
}

// TestRepository_Migrate_AlreadyUpgradedSchema verifies migration succeeds on a schema that already has all current columns.
// It creates fully up-to-date extraction tables manually and executes Migrate.
// It fails if migration returns a PostgreSQL duplicate/already-exists style error.
func TestRepository_Migrate_AlreadyUpgradedSchema(t *testing.T) {
	db, cleanup := newPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	alreadyUpgradedSchema := `
CREATE TABLE impact_service_article_extractions (
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
CREATE UNIQUE INDEX idx_impact_service_article_extractions_article_id ON impact_service_article_extractions(article_id);
CREATE INDEX idx_impact_service_article_extractions_status ON impact_service_article_extractions(extraction_status);
CREATE TABLE impact_service_aggregated_events (
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
CREATE INDEX idx_impact_service_aggregated_events_last_seen_at ON impact_service_aggregated_events(last_seen_at);
CREATE INDEX idx_impact_service_aggregated_events_cluster_key ON impact_service_aggregated_events(cluster_key);
CREATE UNIQUE INDEX idx_impact_service_aggregated_events_cluster_key_unique ON impact_service_aggregated_events(cluster_key);
CREATE TABLE impact_service_clustering_state (
    state_key TEXT PRIMARY KEY,
    last_run_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);`
	if _, err := db.ExecContext(context.Background(), alreadyUpgradedSchema); err != nil {
		t.Fatalf("seed upgraded schema: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate upgraded schema: %v", err)
	}
}

// postgresTableHasColumn checks if a PostgreSQL table has a specific column.
// The t parameter controls test failure behavior, db is the PostgreSQL connection, table is the table name, and column is the desired column name.
// It returns true when the column exists in the table schema.
func postgresTableHasColumn(t *testing.T, db *sql.DB, table string, column string) bool {
	t.Helper()
	var exists int
	err := db.QueryRowContext(
		context.Background(),
		`SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
		table,
		column,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		t.Fatalf("column check %s.%s: %v", table, column, err)
	}
	return exists == 1
}

// TestRepository_UpsertPending verifies pending state upsert persistence.
// It upserts pending state and reads it back by article ID.
// It fails if status/model/timestamp are not stored.
func TestRepository_UpsertPending(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	startedAt := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	if err := repo.UpsertPending(context.Background(), 101, "gpt-4o-mini", startedAt); err != nil {
		t.Fatalf("upsert pending: %v", err)
	}
	rec, err := repo.GetByArticleID(context.Background(), 101)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ExtractionStatus != StatusPending || rec.ExtractionModel != "gpt-4o-mini" {
		t.Fatalf("unexpected record: %+v", rec)
	}
	if rec.ExtractionStartedAt == nil {
		t.Fatal("expected started_at to be set")
	}
}

// TestRepository_UpsertSuccess verifies successful extraction result persistence.
// It writes success state with result payload and reads persisted fields.
// It fails if status or extracted fields are not stored.
func TestRepository_UpsertSuccess(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	finishedAt := time.Date(2026, 3, 28, 10, 5, 0, 0, time.UTC)
	if err := repo.UpsertSuccess(context.Background(), 102, "gpt-4o-mini", finishedAt, validResult(102)); err != nil {
		t.Fatalf("upsert success: %v", err)
	}
	rec, err := repo.GetByArticleID(context.Background(), 102)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ExtractionStatus != StatusDone {
		t.Fatalf("expected done status, got %s", rec.ExtractionStatus)
	}
	if rec.ExtractedEventType == nil || *rec.ExtractedEventType != extract.EventTypeMacro {
		t.Fatal("expected extracted event type")
	}
	if rec.ExtractedGeoCluster == nil || *rec.ExtractedGeoCluster != extract.GeoClusterGlobal {
		t.Fatal("expected extracted geo cluster")
	}
	if rec.ExtractedImpactDirection == nil || *rec.ExtractedImpactDirection != extract.ImpactDirectionNeutral {
		t.Fatal("expected extracted impact direction")
	}
	if rec.ExtractedImpactStrength == nil || *rec.ExtractedImpactStrength != 25 {
		t.Fatal("expected extracted impact strength")
	}
	if rec.ExtractedTimeHorizon == nil || *rec.ExtractedTimeHorizon != extract.TimeHorizonShort {
		t.Fatal("expected extracted time horizon")
	}
	if rec.ExtractedConfidence == nil || *rec.ExtractedConfidence != 0.5 {
		t.Fatal("expected extracted confidence")
	}
	if len(rec.ExtractedCountries) != 1 || rec.ExtractedCountries[0] != "US" {
		t.Fatal("expected extracted countries to persist")
	}
	if len(rec.ExtractedCompanies) != 1 || rec.ExtractedCompanies[0] != "ACME" {
		t.Fatal("expected extracted companies to persist")
	}
	if len(rec.ExtractedSectors) != 1 || rec.ExtractedSectors[0] != "industrials" {
		t.Fatal("expected extracted sectors to persist")
	}
	if len(rec.ExtractedIndustries) != 1 || rec.ExtractedIndustries[0] != "Software - Application" {
		t.Fatal("expected extracted industries to persist")
	}
	if len(rec.ExtractedChannels) != 1 || rec.ExtractedChannels[0] != extract.ChannelRiskSentiment {
		t.Fatal("expected extracted channels to persist")
	}
}

// TestRepository_UpsertFailure verifies failed extraction state persistence.
// It writes failure state with error message and reads it back.
// It fails if failed status or error message are missing.
func TestRepository_UpsertFailure(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	finishedAt := time.Date(2026, 3, 28, 10, 6, 0, 0, time.UTC)
	if err := repo.UpsertFailure(context.Background(), 103, "gpt-4o-mini", finishedAt, "extractor failed"); err != nil {
		t.Fatalf("upsert failure: %v", err)
	}
	rec, err := repo.GetByArticleID(context.Background(), 103)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ExtractionStatus != StatusFailed {
		t.Fatalf("expected failed status, got %s", rec.ExtractionStatus)
	}
	if rec.ExtractionError == nil || *rec.ExtractionError != "extractor failed" {
		t.Fatal("expected extraction error to persist")
	}
}

// TestRepository_IdempotentUpsertByArticleID verifies repeated upserts update same article row idempotently.
// It applies pending then success on same article ID and reads final row.
// It fails if duplicate rows or stale status remain.
func TestRepository_IdempotentUpsertByArticleID(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	if err := repo.UpsertPending(context.Background(), 104, "gpt-4o-mini", time.Now().UTC()); err != nil {
		t.Fatalf("upsert pending: %v", err)
	}
	if err := repo.UpsertSuccess(context.Background(), 104, "gpt-4o-mini", time.Now().UTC(), validResult(104)); err != nil {
		t.Fatalf("upsert success: %v", err)
	}
	rec, err := repo.GetByArticleID(context.Background(), 104)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ExtractionStatus != StatusDone {
		t.Fatalf("expected status done after update, got %s", rec.ExtractionStatus)
	}
}

// TestRepository_TransitionPendingToFailed verifies lifecycle transition from pending to failed persists latest state.
// It first writes pending state then writes failed state on the same article ID.
// It fails if final status/error do not reflect failed transition.
func TestRepository_TransitionPendingToFailed(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	if err := repo.UpsertPending(context.Background(), 105, "gpt-4o-mini", time.Now().UTC()); err != nil {
		t.Fatalf("upsert pending: %v", err)
	}
	if err := repo.UpsertFailure(context.Background(), 105, "gpt-4o-mini", time.Now().UTC(), "boom"); err != nil {
		t.Fatalf("upsert failure: %v", err)
	}

	rec, err := repo.GetByArticleID(context.Background(), 105)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if rec.ExtractionStatus != StatusFailed {
		t.Fatalf("expected failed status, got %s", rec.ExtractionStatus)
	}
	if rec.ExtractionError == nil || *rec.ExtractionError != "boom" {
		t.Fatalf("expected persisted failure error, got %+v", rec.ExtractionError)
	}
}

// TestRepository_GetByArticleID_NotFound verifies missing rows return sql.ErrNoRows wrapped.
// It loads a non-existent article ID.
// It fails if not-found condition is not exposed.
func TestRepository_GetByArticleID_NotFound(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	_, err := repo.GetByArticleID(context.Background(), 999)
	if err == nil {
		t.Fatal("expected error for missing row")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

// TestRepository_GetByArticleID_InvalidJSON verifies malformed stored JSON fields are rejected on read.
// It inserts a row with invalid extracted_countries JSON directly into table.
// It fails if repository silently swallows decode errors.
func TestRepository_GetByArticleID_InvalidJSON(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	_, err := repo.db.ExecContext(
		context.Background(),
		`INSERT INTO impact_service_article_extractions (
			article_id, extraction_status, extraction_model, extraction_started_at, extraction_finished_at, extraction_error,
			extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_companies, extracted_sectors, extracted_industries,
			extracted_impact_direction, extracted_impact_strength, extracted_channels, extracted_time_horizon,
			extracted_confidence, created_at, updated_at
		) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		111,
		StatusDone,
		"gpt-4o-mini",
		"{bad-json",
		time.Now().UTC(),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("insert malformed row: %v", err)
	}

	if _, err := repo.GetByArticleID(context.Background(), 111); err == nil {
		t.Fatal("expected decode error for malformed JSON")
	}
}

// TestRepository_ClaimPending_FirstClaimGranted verifies first claimant obtains pending execution claim.
// It calls ClaimPending for a non-existing article row.
// It fails if initial claim is not granted.
func TestRepository_ClaimPending_FirstClaimGranted(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	claim, err := repo.ClaimPending(context.Background(), 106, "gpt-4o-mini", time.Now().UTC())
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if claim.Outcome != ClaimOutcomeGranted {
		t.Fatalf("expected granted outcome, got %s", claim.Outcome)
	}
}

// TestRepository_ClaimPending_ExistingStatusOutcomes verifies denied claim outcomes match existing row status.
// It pre-populates done/pending/failed rows and calls ClaimPending for each.
// It fails if returned claim outcomes do not reflect existing states.
func TestRepository_ClaimPending_ExistingStatusOutcomes(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	if err := repo.UpsertSuccess(context.Background(), 107, "gpt-4o-mini", time.Now().UTC(), validResult(107)); err != nil {
		t.Fatalf("seed done: %v", err)
	}
	if err := repo.UpsertPending(context.Background(), 108, "gpt-4o-mini", time.Now().UTC()); err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	if err := repo.UpsertFailure(context.Background(), 109, "gpt-4o-mini", time.Now().UTC(), "boom"); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	doneClaim, err := repo.ClaimPending(context.Background(), 107, "gpt-4o-mini", time.Now().UTC())
	if err != nil {
		t.Fatalf("claim done: %v", err)
	}
	if doneClaim.Outcome != ClaimOutcomeAlreadyDone {
		t.Fatalf("expected already_done, got %s", doneClaim.Outcome)
	}

	pendingClaim, err := repo.ClaimPending(context.Background(), 108, "gpt-4o-mini", time.Now().UTC())
	if err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if pendingClaim.Outcome != ClaimOutcomeAlreadyPending {
		t.Fatalf("expected already_pending, got %s", pendingClaim.Outcome)
	}

	failedClaim, err := repo.ClaimPending(context.Background(), 109, "gpt-4o-mini", time.Now().UTC())
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}
	if failedClaim.Outcome != ClaimOutcomeAlreadyFailed {
		t.Fatalf("expected already_failed, got %s", failedClaim.Outcome)
	}
}

// TestRepository_ClaimPending_ConcurrentSingleWinner verifies concurrent claims allow exactly one granted claimant.
// It executes concurrent ClaimPending calls for the same article_id.
// It fails if more than one claimant is granted.
func TestRepository_ClaimPending_ConcurrentSingleWinner(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	const workers = 10
	var granted int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claim, err := repo.ClaimPending(context.Background(), 110, "gpt-4o-mini", time.Now().UTC())
			if err != nil {
				t.Errorf("claim pending: %v", err)
				return
			}
			if claim.Outcome == ClaimOutcomeGranted {
				atomic.AddInt32(&granted, 1)
			}
		}()
	}
	wg.Wait()
	if granted != 1 {
		t.Fatalf("expected exactly one granted claimant, got %d", granted)
	}
}
