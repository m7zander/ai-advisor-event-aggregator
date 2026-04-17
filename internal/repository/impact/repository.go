package impactrepo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	impactdomain "ai-advisor-impact-service/internal/impact"
	"ai-advisor-impact-service/internal/logging"
	"ai-advisor-impact-service/internal/observability"
)

const impactSchemaSetup = `
CREATE TABLE IF NOT EXISTS impact_service_event_security_impacts (
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
);
CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_event_id ON impact_service_event_security_impacts(event_id);
CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_security_code ON impact_service_event_security_impacts(security_code);
CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_impact_score_desc ON impact_service_event_security_impacts(impact_score DESC);`

const impactLegacyRenameSQL = `
ALTER TABLE IF EXISTS event_security_impacts RENAME TO impact_service_event_security_impacts;
DO $$
BEGIN
    IF to_regclass('idx_event_security_impacts_event_id') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_event_id') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_event_id RENAME TO idx_impact_service_event_security_impacts_event_id;
    END IF;
    IF to_regclass('idx_event_security_impacts_security_code') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_security_code') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_security_code RENAME TO idx_impact_service_event_security_impacts_security_code;
    END IF;
    IF to_regclass('idx_event_security_impacts_impact_score_desc') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_impact_score_desc') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_impact_score_desc RENAME TO idx_impact_service_event_security_impacts_impact_score_desc;
    END IF;
END $$;`

var impactRepoLogger = logging.New()

type Repository struct {
	db *sql.DB
}

type EventImpactFilter struct {
	Limit     int
	Offset    int
	MinScore  float64
	Direction *impactdomain.ImpactDirection
}

type SecurityImpactFilter struct {
	Limit    int
	Offset   int
	MinScore float64
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	return &Repository{db: db}, nil
}

func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.db.ExecContext(ctx, impactLegacyRenameSQL); err != nil {
		return fmt.Errorf("rename legacy impact tables: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, impactSchemaSetup); err != nil {
		return fmt.Errorf("migrate impact schema: %w", err)
	}
	return nil
}

func (r *Repository) UpsertEventSecurityImpacts(ctx context.Context, impacts []impactdomain.EventSecurityImpact) error {
	ctx, span := observability.StartSpan(ctx, "repository.impact.upsert_event_security_impacts")
	defer span.End()

	if len(impacts) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("begin impact upsert tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := r.upsertImpactsInTx(ctx, tx, impacts); err != nil {
		observability.RecordError(span, err)
		return err
	}
	if err = tx.Commit(); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("commit impact upsert tx: %w", err)
	}
	return nil
}

func (r *Repository) ReplaceAllEventSecurityImpacts(ctx context.Context, impacts []impactdomain.EventSecurityImpact) error {
	ctx, span := observability.StartSpan(ctx, "repository.impact.replace_all_event_security_impacts")
	defer span.End()

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("begin impact replace tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = tx.ExecContext(ctx, `DELETE FROM impact_service_event_security_impacts`); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("delete stale impacts: %w", err)
	}
	if len(impacts) > 0 {
		if err := r.upsertImpactsInTx(ctx, tx, impacts); err != nil {
			observability.RecordError(span, err)
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		observability.RecordError(span, err)
		return fmt.Errorf("commit impact replace tx: %w", err)
	}
	return nil
}

func (r *Repository) upsertImpactsInTx(ctx context.Context, tx *sql.Tx, impacts []impactdomain.EventSecurityImpact) error {
	const q = `INSERT INTO impact_service_event_security_impacts (
event_id, security_code, isin, security_name, impact_direction, impact_score, impact_confidence,
geo_match_score, country_match_score, sector_match_score, event_type_match_score, profile_confidence,
rule_version, explanation_codes, computed_at
) VALUES (
$1, $2, $3, $4, $5, $6, $7,
$8, $9, $10, $11, $12,
$13, $14, $15
)
	ON CONFLICT (event_id, security_code, rule_version) DO UPDATE SET
isin=excluded.isin,
security_name=excluded.security_name,
impact_direction=excluded.impact_direction,
impact_score=excluded.impact_score,
impact_confidence=excluded.impact_confidence,
geo_match_score=excluded.geo_match_score,
country_match_score=excluded.country_match_score,
sector_match_score=excluded.sector_match_score,
event_type_match_score=excluded.event_type_match_score,
profile_confidence=excluded.profile_confidence,
explanation_codes=excluded.explanation_codes,
computed_at=excluded.computed_at`

	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare impact upsert: %w", err)
	}
	defer func() {
		if cerr := stmt.Close(); cerr != nil {
			impactRepoLogger.ErrorWithContract(ctx, "repository.impact.stmt_close_failed", "repository/impact", "failed to close statement", cerr, logging.ErrorContract{
				Failure:        "impact_repository_statement_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"statement":"impact_upsert"}`,
				Reaction:       "statement close failure logged",
			})
		}
	}()

	for i := range impacts {
		imp := impacts[i]
		if strings.TrimSpace(imp.EventID) == "" || strings.TrimSpace(imp.SecurityCode) == "" || strings.TrimSpace(imp.RuleVersion) == "" {
			return fmt.Errorf("invalid impact row at index %d: event_id/security_code/rule_version required", i)
		}
		codesJSON, marshalErr := json.Marshal(imp.ExplanationCodes)
		if marshalErr != nil {
			return fmt.Errorf("marshal explanation_codes at index %d: %w", i, marshalErr)
		}
		if _, err = stmt.ExecContext(ctx,
			imp.EventID, imp.SecurityCode, imp.ISIN, imp.SecurityName, imp.ImpactDirection.String(), imp.ImpactScore, imp.ImpactConfidence,
			imp.GeoMatchScore, imp.CountryMatchScore, imp.SectorMatchScore, imp.EventTypeMatchScore, imp.ProfileConfidence,
			imp.RuleVersion, string(codesJSON), imp.ComputedAt.UTC(),
		); err != nil {
			return fmt.Errorf("upsert impact row at index %d: %w", i, err)
		}
	}
	return nil
}

func (r *Repository) GetSecuritiesByEvent(ctx context.Context, eventID string, filter EventImpactFilter) ([]impactdomain.EventSecurityImpact, error) {
	ctx, span := observability.StartSpan(ctx, "repository.impact.get_securities_by_event")
	defer span.End()

	if strings.TrimSpace(eventID) == "" {
		return nil, fmt.Errorf("eventID is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	q := `SELECT event_id, security_code, isin, security_name, impact_direction, impact_score, impact_confidence,
geo_match_score, country_match_score, sector_match_score, event_type_match_score, profile_confidence,
rule_version, explanation_codes, computed_at
FROM impact_service_event_security_impacts
WHERE event_id = $1 AND impact_score >= $2`
	args := []any{strings.TrimSpace(eventID), filter.MinScore}
	if filter.Direction != nil {
		args = append(args, filter.Direction.String())
		q += fmt.Sprintf(" AND impact_direction = $%d", len(args))
	}
	args = append(args, filter.Limit, filter.Offset)
	q += fmt.Sprintf(" ORDER BY impact_score DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	out, err := r.queryImpacts(ctx, q, args...)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return out, nil
}

func (r *Repository) GetImpactsBySecurity(ctx context.Context, code string) ([]impactdomain.EventSecurityImpact, error) {
	ctx, span := observability.StartSpan(ctx, "repository.impact.get_impacts_by_security")
	defer span.End()

	if strings.TrimSpace(code) == "" {
		return nil, fmt.Errorf("code is required")
	}
	q := `SELECT event_id, security_code, isin, security_name, impact_direction, impact_score, impact_confidence,
geo_match_score, country_match_score, sector_match_score, event_type_match_score, profile_confidence,
rule_version, explanation_codes, computed_at
FROM impact_service_event_security_impacts
WHERE security_code = $1
ORDER BY impact_score DESC`
	out, err := r.queryImpacts(ctx, q, strings.TrimSpace(code))
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return out, nil
}

func (r *Repository) ListEventSecurityImpacts(ctx context.Context, eventID string, filter EventImpactFilter) ([]impactdomain.EventSecurityImpact, error) {
	return r.GetSecuritiesByEvent(ctx, eventID, filter)
}

func (r *Repository) ListSecurityImpacts(ctx context.Context, code string, filter SecurityImpactFilter) ([]impactdomain.EventSecurityImpact, error) {
	ctx, span := observability.StartSpan(ctx, "repository.impact.list_security_impacts")
	defer span.End()

	if strings.TrimSpace(code) == "" {
		return nil, fmt.Errorf("code is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}
	q := `SELECT event_id, security_code, isin, security_name, impact_direction, impact_score, impact_confidence,
geo_match_score, country_match_score, sector_match_score, event_type_match_score, profile_confidence,
rule_version, explanation_codes, computed_at
FROM impact_service_event_security_impacts
WHERE security_code = $1 AND impact_score >= $2
ORDER BY impact_score DESC
LIMIT $3 OFFSET $4`
	out, err := r.queryImpacts(ctx, q, strings.TrimSpace(code), filter.MinScore, filter.Limit, filter.Offset)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	return out, nil
}

func (r *Repository) CountImpactsByEventIDs(ctx context.Context, eventIDs []string) (map[string]int, error) {
	ctx, span := observability.StartSpan(ctx, "repository.impact.count_impacts_by_event_ids")
	defer span.End()

	out := make(map[string]int, len(eventIDs))
	if len(eventIDs) == 0 {
		return out, nil
	}
	const q = `SELECT event_id, COUNT(*) FROM impact_service_event_security_impacts WHERE event_id = ANY($1) GROUP BY event_id`
	rows, err := r.db.QueryContext(ctx, q, eventIDs)
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("count impacts by event ids: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			impactRepoLogger.ErrorWithContract(ctx, "repository.impact.rows_close_failed", "repository/impact", "failed to close rows", cerr, logging.ErrorContract{
				Failure:        "impact_repository_rows_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"query":"get_securities_by_event"}`,
				Reaction:       "rows close failure logged",
			})
		}
	}()
	for rows.Next() {
		var eventID string
		var count int
		if err := rows.Scan(&eventID, &count); err != nil {
			observability.RecordError(span, err)
			return nil, fmt.Errorf("scan count row: %w", err)
		}
		out[eventID] = count
	}
	if err := rows.Err(); err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("iterate count rows: %w", err)
	}
	return out, nil
}

func (r *Repository) queryImpacts(ctx context.Context, query string, args ...any) ([]impactdomain.EventSecurityImpact, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query impacts: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			impactRepoLogger.ErrorWithContract(ctx, "repository.impact.rows_close_failed", "repository/impact", "failed to close rows", cerr, logging.ErrorContract{
				Failure:        "impact_repository_rows_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"query":"get_impacts_by_security"}`,
				Reaction:       "rows close failure logged",
			})
		}
	}()

	impacts := make([]impactdomain.EventSecurityImpact, 0)
	for rows.Next() {
		var row impactdomain.EventSecurityImpact
		var directionRaw string
		var explanationCodesRaw []byte
		var computedAt time.Time
		if err := rows.Scan(
			&row.EventID,
			&row.SecurityCode,
			&row.ISIN,
			&row.SecurityName,
			&directionRaw,
			&row.ImpactScore,
			&row.ImpactConfidence,
			&row.GeoMatchScore,
			&row.CountryMatchScore,
			&row.SectorMatchScore,
			&row.EventTypeMatchScore,
			&row.ProfileConfidence,
			&row.RuleVersion,
			&explanationCodesRaw,
			&computedAt,
		); err != nil {
			return nil, fmt.Errorf("scan impact row: %w", err)
		}
		row.ImpactDirection = impactdomain.ImpactDirection(directionRaw)
		if !row.ImpactDirection.IsValid() {
			return nil, fmt.Errorf("invalid impact direction in DB: %q", directionRaw)
		}
		if err := json.Unmarshal(explanationCodesRaw, &row.ExplanationCodes); err != nil {
			return nil, fmt.Errorf("decode explanation codes: %w", err)
		}
		row.ComputedAt = computedAt.UTC()
		impacts = append(impacts, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate impact rows: %w", err)
	}
	return impacts, nil
}
