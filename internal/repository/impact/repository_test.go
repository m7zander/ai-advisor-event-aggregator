package impactrepo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	impactdomain "ai-advisor-impact-service/internal/impact"
	_ "github.com/lib/pq"
)

var impactTestSchemaCounter uint64

func newImpactTestRepository(t *testing.T) (*Repository, func()) {
	t.Helper()
	db, cleanup := newImpactPostgresTestDB(t)
	repo, err := NewRepository(db)
	if err != nil {
		cleanup()
		t.Fatalf("new repo: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		cleanup()
		t.Fatalf("migrate: %v", err)
	}
	return repo, cleanup
}

func newImpactPostgresTestDB(t *testing.T) (*sql.DB, func()) {
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
	schema := fmt.Sprintf("impact_test_%d_%d", time.Now().UnixNano(), atomic.AddUint64(&impactTestSchemaCounter, 1))
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

func TestRepository_UpsertAndQuery(t *testing.T) {
	repo, cleanup := newImpactTestRepository(t)
	defer cleanup()

	now := time.Now().UTC()
	first := impactdomain.EventSecurityImpact{
		EventID:             "evt-1",
		SecurityCode:        "AAA",
		ISIN:                "ISIN1",
		SecurityName:        "Alpha",
		ImpactDirection:     impactdomain.ImpactDirectionPositive,
		ImpactScore:         42,
		ImpactConfidence:    0.8,
		GeoMatchScore:       0.5,
		CountryMatchScore:   0.2,
		SectorMatchScore:    0.9,
		EventTypeMatchScore: 0.7,
		ProfileConfidence:   0.8,
		RuleVersion:         "impact_rules_v1",
		ExplanationCodes:    []string{"event_type:macro", "rule:impact_rules_v1"},
		ComputedAt:          now,
	}
	if err := repo.UpsertEventSecurityImpacts(context.Background(), []impactdomain.EventSecurityImpact{first}); err != nil {
		t.Fatalf("UpsertEventSecurityImpacts() error = %v", err)
	}

	updated := first
	updated.ImpactScore = 77
	if err := repo.UpsertEventSecurityImpacts(context.Background(), []impactdomain.EventSecurityImpact{updated}); err != nil {
		t.Fatalf("UpsertEventSecurityImpacts() update error = %v", err)
	}

	byEvent, err := repo.GetSecuritiesByEvent(context.Background(), "evt-1", EventImpactFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetSecuritiesByEvent() error = %v", err)
	}
	if len(byEvent) != 1 || byEvent[0].ImpactScore != 77 {
		t.Fatalf("unexpected byEvent: %+v", byEvent)
	}

	bySecurity, err := repo.GetImpactsBySecurity(context.Background(), "AAA")
	if err != nil {
		t.Fatalf("GetImpactsBySecurity() error = %v", err)
	}
	if len(bySecurity) != 1 || bySecurity[0].EventID != "evt-1" {
		t.Fatalf("unexpected bySecurity: %+v", bySecurity)
	}
}

func TestRepository_Migrate_RenamesLegacyImpactTableToServicePrefix(t *testing.T) {
	db, cleanup := newImpactPostgresTestDB(t)
	defer cleanup()

	repo, err := NewRepository(db)
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}

	legacySchema := `
CREATE TABLE event_security_impacts (
    event_id TEXT NOT NULL,
    security_code TEXT NOT NULL,
    isin TEXT NOT NULL,
    security_name TEXT NOT NULL,
    impact_direction TEXT NOT NULL,
    impact_score DOUBLE PRECISION NOT NULL,
    impact_confidence DOUBLE PRECISION NOT NULL,
    geo_match_score DOUBLE PRECISION NOT NULL,
    country_match_score DOUBLE PRECISION NOT NULL,
    sector_match_score DOUBLE PRECISION NOT NULL,
    event_type_match_score DOUBLE PRECISION NOT NULL,
    profile_confidence DOUBLE PRECISION NOT NULL,
    rule_version TEXT NOT NULL,
    explanation_codes JSONB NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, security_code, rule_version)
);`
	if _, err := db.ExecContext(context.Background(), legacySchema); err != nil {
		t.Fatalf("seed legacy impact table: %v", err)
	}

	if err := repo.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate legacy impact table rename: %v", err)
	}

	var exists int
	if err := db.QueryRowContext(
		context.Background(),
		`SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`,
		"impact_service_event_security_impacts",
	).Scan(&exists); err != nil {
		t.Fatalf("check canonical impact table existence: %v", err)
	}
}

func TestRepository_GetSecuritiesByEvent_FilterDirectionAndMinScore(t *testing.T) {
	repo, cleanup := newImpactTestRepository(t)
	defer cleanup()

	now := time.Now().UTC()
	rows := []impactdomain.EventSecurityImpact{
		{
			EventID:             "evt-2",
			SecurityCode:        "A",
			ISIN:                "ISIN-A",
			SecurityName:        "A",
			ImpactDirection:     impactdomain.ImpactDirectionPositive,
			ImpactScore:         80,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
		{
			EventID:             "evt-2",
			SecurityCode:        "B",
			ISIN:                "ISIN-B",
			SecurityName:        "B",
			ImpactDirection:     impactdomain.ImpactDirectionNegative,
			ImpactScore:         20,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
	}
	if err := repo.UpsertEventSecurityImpacts(context.Background(), rows); err != nil {
		t.Fatalf("upsert rows: %v", err)
	}

	direction := impactdomain.ImpactDirectionPositive
	got, err := repo.GetSecuritiesByEvent(context.Background(), "evt-2", EventImpactFilter{
		Limit:     10,
		MinScore:  50,
		Direction: &direction,
	})
	if err != nil {
		t.Fatalf("GetSecuritiesByEvent() error = %v", err)
	}
	if len(got) != 1 || got[0].SecurityCode != "A" {
		t.Fatalf("unexpected filtered result: %+v", got)
	}
}

func TestRepository_ListSecurityImpacts_OrderedByScoreDesc(t *testing.T) {
	repo, cleanup := newImpactTestRepository(t)
	defer cleanup()

	now := time.Now().UTC()
	rows := []impactdomain.EventSecurityImpact{
		{
			EventID:             "evt-1",
			SecurityCode:        "AAA",
			ISIN:                "ISIN-A",
			SecurityName:        "A",
			ImpactDirection:     impactdomain.ImpactDirectionPositive,
			ImpactScore:         25,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
		{
			EventID:             "evt-2",
			SecurityCode:        "AAA",
			ISIN:                "ISIN-A",
			SecurityName:        "A",
			ImpactDirection:     impactdomain.ImpactDirectionNegative,
			ImpactScore:         55,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
	}
	if err := repo.UpsertEventSecurityImpacts(context.Background(), rows); err != nil {
		t.Fatalf("upsert rows: %v", err)
	}

	got, err := repo.ListSecurityImpacts(context.Background(), "AAA", SecurityImpactFilter{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("ListSecurityImpacts() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].EventID != "evt-2" || got[1].EventID != "evt-1" {
		t.Fatalf("expected score-desc ordering, got %+v", got)
	}
}

func TestRepository_ReplaceAllEventSecurityImpacts_DeletesObsoleteRows(t *testing.T) {
	repo, cleanup := newImpactTestRepository(t)
	defer cleanup()

	now := time.Now().UTC()
	seed := []impactdomain.EventSecurityImpact{
		{
			EventID:             "evt-old",
			SecurityCode:        "AAA",
			ISIN:                "ISIN-A",
			SecurityName:        "A",
			ImpactDirection:     impactdomain.ImpactDirectionPositive,
			ImpactScore:         22,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
	}
	if err := repo.UpsertEventSecurityImpacts(context.Background(), seed); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
	next := []impactdomain.EventSecurityImpact{
		{
			EventID:             "evt-new",
			SecurityCode:        "BBB",
			ISIN:                "ISIN-B",
			SecurityName:        "B",
			ImpactDirection:     impactdomain.ImpactDirectionNegative,
			ImpactScore:         44,
			ImpactConfidence:    0.9,
			GeoMatchScore:       0.5,
			CountryMatchScore:   0.5,
			SectorMatchScore:    0.5,
			EventTypeMatchScore: 0.5,
			ProfileConfidence:   0.9,
			RuleVersion:         "impact_rules_v1",
			ExplanationCodes:    []string{"rule:impact_rules_v1"},
			ComputedAt:          now,
		},
	}
	if err := repo.ReplaceAllEventSecurityImpacts(context.Background(), next); err != nil {
		t.Fatalf("ReplaceAllEventSecurityImpacts() error = %v", err)
	}
	oldRows, err := repo.GetImpactsBySecurity(context.Background(), "AAA")
	if err != nil {
		t.Fatalf("GetImpactsBySecurity(AAA) error = %v", err)
	}
	if len(oldRows) != 0 {
		t.Fatalf("expected obsolete rows deleted, got %+v", oldRows)
	}
	newRows, err := repo.GetImpactsBySecurity(context.Background(), "BBB")
	if err != nil {
		t.Fatalf("GetImpactsBySecurity(BBB) error = %v", err)
	}
	if len(newRows) != 1 || newRows[0].EventID != "evt-new" {
		t.Fatalf("unexpected replaced rows: %+v", newRows)
	}
}
