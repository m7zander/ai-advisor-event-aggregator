// Package extractionrepo provides persistence for deterministic event clustering inputs and outputs.
package extractionrepo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/logging"
	"ai-advisor-event-aggregator/internal/observability"
)

const clusteringCursorKey = "events_cluster_last_run_at"

var extractionRepoLogger = logging.New()

// SuccessfulExtraction represents one eligible successful extraction used as deterministic clustering input.
type SuccessfulExtraction struct {
	ArticleID       int64
	EventType       extract.EventType
	GeoCluster      extract.GeoCluster
	Countries       []string
	Sectors         []string
	Industries      []string
	ImpactDirection extract.ImpactDirection
	ImpactStrength  int
	Confidence      float64
	FinishedAt      time.Time
}

// ListSuccessfulExtractionsSince loads successful extraction rows strictly after the provided timestamp.
// The since parameter is an exclusive lower bound on extraction_finished_at and results are sorted ascending.
// It returns rows with fully populated extracted fields (including countries, sectors, and industries) required by deterministic clustering.
func (r *Repository) ListSuccessfulExtractionsSince(ctx context.Context, since time.Time) ([]SuccessfulExtraction, error) {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.list_successful_extractions_since")
	defer span.End()

	const q = `SELECT article_id, extracted_event_type, extracted_geo_cluster, extracted_countries, extracted_sectors, extracted_industries,
extracted_impact_direction, extracted_impact_strength, extracted_confidence, extraction_finished_at
FROM impact_service_article_extractions
WHERE extraction_status = $1
  AND extraction_finished_at IS NOT NULL
  AND extraction_finished_at > $2
  AND extracted_event_type IS NOT NULL
  AND extracted_geo_cluster IS NOT NULL
  AND extracted_impact_direction IS NOT NULL
  AND extracted_impact_strength IS NOT NULL
  AND extracted_confidence IS NOT NULL
ORDER BY extraction_finished_at ASC, article_id ASC`

	rows, err := r.db.QueryContext(ctx, q, StatusDone, since.UTC())
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("list successful extractions since: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			extractionRepoLogger.ErrorWithContract(ctx, "repository.rows_close_failed", "repository/extraction", "failed to close query rows", cerr, logging.ErrorContract{
				Failure:        "extraction_repository_rows_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"query_id":"list_successful_extractions_since"}`,
				Reaction:       "rows close failure logged",
			},
				logging.Field{Key: "use_case", Value: "list_successful_extractions_since"},
				logging.Field{Key: "query_id", Value: "list_successful_extractions_since"},
			)
		}
	}()

	items := make([]SuccessfulExtraction, 0)
	for rows.Next() {
		var item SuccessfulExtraction
		var eventTypeRaw, geoClusterRaw, impactDirectionRaw string
		var countriesRaw, sectorsRaw, industriesRaw sql.NullString
		var finishedAt time.Time
		if err := rows.Scan(
			&item.ArticleID,
			&eventTypeRaw,
			&geoClusterRaw,
			&countriesRaw,
			&sectorsRaw,
			&industriesRaw,
			&impactDirectionRaw,
			&item.ImpactStrength,
			&item.Confidence,
			&finishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan successful extraction: %w", err)
		}
		item.EventType = extract.EventType(eventTypeRaw)
		item.GeoCluster = extract.GeoCluster(geoClusterRaw)
		item.ImpactDirection = extract.ImpactDirection(impactDirectionRaw)
		item.FinishedAt = finishedAt.UTC()
		if countriesRaw.Valid {
			if err := json.Unmarshal([]byte(countriesRaw.String), &item.Countries); err != nil {
				observability.RecordError(span, err)
				return nil, fmt.Errorf("decode extracted_countries for article_id=%d: %w", item.ArticleID, err)
			}
		}
		if sectorsRaw.Valid {
			if err := json.Unmarshal([]byte(sectorsRaw.String), &item.Sectors); err != nil {
				observability.RecordError(span, err)
				return nil, fmt.Errorf("decode extracted_sectors for article_id=%d: %w", item.ArticleID, err)
			}
		}
		if industriesRaw.Valid {
			if err := json.Unmarshal([]byte(industriesRaw.String), &item.Industries); err != nil {
				observability.RecordError(span, err)
				return nil, fmt.Errorf("decode extracted_industries for article_id=%d: %w", item.ArticleID, err)
			}
		}
		if item.Countries == nil {
			item.Countries = []string{}
		}
		if item.Sectors == nil {
			item.Sectors = []string{}
		}
		if item.Industries == nil {
			item.Industries = []string{}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("iterate successful extractions: %w", err)
	}
	return items, nil
}

// GetEventByClusterKey loads one aggregated event by deterministic cluster key.
// The clusterKey parameter identifies a persistent event semantic cluster.
// It returns the event, a found flag, and an error when query/decode fails.
func (r *Repository) GetEventByClusterKey(ctx context.Context, clusterKey string) (event.Event, bool, error) {
	const q = `SELECT event_id, cluster_key, event_type, geo_cluster, countries, sectors, industries,
 direction, strength, confidence, status, article_ids, source_count,
 first_seen_at, last_seen_at, created_at, updated_at
FROM impact_service_aggregated_events
WHERE cluster_key = $1`
	row := r.db.QueryRowContext(ctx, q, clusterKey)
	evt, found, err := scanEventRow(row.Scan)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("get aggregated event by cluster key: %w", err)
	}
	return evt, found, nil
}

// GetEventByID loads one aggregated event by event_id.
// The eventID parameter identifies the persisted aggregated event row.
// It returns event, found flag, and an error when query/decode fails.
func (r *Repository) GetEventByID(ctx context.Context, eventID string) (event.Event, bool, error) {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.get_event_by_id")
	defer span.End()

	const q = `SELECT event_id, cluster_key, event_type, geo_cluster, countries, sectors, industries,
 direction, strength, confidence, status, article_ids, source_count,
 first_seen_at, last_seen_at, created_at, updated_at
FROM impact_service_aggregated_events
WHERE event_id = $1`
	row := r.db.QueryRowContext(ctx, q, eventID)
	evt, found, err := scanEventRow(row.Scan)
	if err != nil {
		observability.RecordError(span, err)
		return event.Event{}, false, fmt.Errorf("get aggregated event by id: %w", err)
	}
	return evt, found, nil
}

// UpsertEventByClusterKey persists one aggregated event deterministically by cluster_key.
// The evt parameter contains the fully aggregated event payload produced by incremental deterministic clustering.
// It returns an error when validation, serialization, or upsert execution fails.
func (r *Repository) UpsertEventByClusterKey(ctx context.Context, evt event.Event) error {
	if err := evt.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}

	countriesJSON, err := json.Marshal(evt.Countries)
	if err != nil {
		return fmt.Errorf("marshal countries: %w", err)
	}
	sectorsJSON, err := json.Marshal(evt.Sectors)
	if err != nil {
		return fmt.Errorf("marshal sectors: %w", err)
	}
	industriesJSON, err := json.Marshal(evt.Industries)
	if err != nil {
		return fmt.Errorf("marshal industries: %w", err)
	}
	articleIDsJSON, err := json.Marshal(evt.ArticleIDs)
	if err != nil {
		return fmt.Errorf("marshal article_ids: %w", err)
	}

	const q = `INSERT INTO impact_service_aggregated_events (
 event_id, cluster_key, event_type, geo_cluster, countries, sectors, industries,
 direction, strength, confidence, status, article_ids, source_count,
 first_seen_at, last_seen_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT(cluster_key) DO UPDATE SET
 event_id=excluded.event_id,
 event_type=excluded.event_type,
 geo_cluster=excluded.geo_cluster,
 countries=excluded.countries,
 sectors=excluded.sectors,
 industries=excluded.industries,
 direction=excluded.direction,
 strength=excluded.strength,
 confidence=excluded.confidence,
 status=excluded.status,
 article_ids=excluded.article_ids,
 source_count=excluded.source_count,
 first_seen_at=excluded.first_seen_at,
 last_seen_at=excluded.last_seen_at,
 updated_at=excluded.updated_at`
	if _, err := r.db.ExecContext(ctx, q,
		evt.ID,
		evt.ClusterKey,
		string(evt.EventType),
		string(evt.GeoCluster),
		string(countriesJSON),
		string(sectorsJSON),
		string(industriesJSON),
		string(evt.Direction),
		evt.Strength,
		evt.Confidence,
		evt.Status,
		string(articleIDsJSON),
		evt.SourceCount,
		evt.FirstSeenAt.UTC(),
		evt.LastSeenAt.UTC(),
		evt.CreatedAt.UTC(),
		evt.UpdatedAt.UTC(),
	); err != nil {
		return fmt.Errorf("upsert aggregated event by cluster key: %w", err)
	}
	return nil
}

// ListEvents loads aggregated events using optional closed timestamp bounds on last_seen_at.
// The limit parameter defines max returned rows and since/until are optional inclusive bounds.
// It returns events sorted by last_seen_at descending then event_id descending.
func (r *Repository) ListEvents(ctx context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error) {
	ctx, span := observability.StartSpan(ctx, "repository.extraction.list_events")
	defer span.End()

	if limit <= 0 {
		err := fmt.Errorf("limit must be > 0")
		observability.RecordError(span, err)
		return nil, err
	}
	query := `SELECT event_id, cluster_key, event_type, geo_cluster, countries, sectors, industries,
 direction, strength, confidence, status, article_ids, source_count,
 first_seen_at, last_seen_at, created_at, updated_at
FROM impact_service_aggregated_events`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if since != nil {
		args = append(args, since.UTC())
		clauses = append(clauses, fmt.Sprintf("last_seen_at >= $%d", len(args)))
	}
	if until != nil {
		args = append(args, until.UTC())
		clauses = append(clauses, fmt.Sprintf("last_seen_at <= $%d", len(args)))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY last_seen_at DESC, event_id DESC LIMIT $%d", len(args))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("list aggregated events: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			extractionRepoLogger.ErrorWithContract(ctx, "repository.rows_close_failed", "repository/extraction", "failed to close query rows", cerr, logging.ErrorContract{
				Failure:        "extraction_repository_rows_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"query_id":"list_impact_service_aggregated_events"}`,
				Reaction:       "rows close failure logged",
			},
				logging.Field{Key: "use_case", Value: "list_events"},
				logging.Field{Key: "query_id", Value: "list_impact_service_aggregated_events"},
			)
		}
	}()

	events := make([]event.Event, 0, limit)
	for rows.Next() {
		evt, found, scanErr := scanEventRow(rows.Scan)
		if scanErr != nil {
			observability.RecordError(span, scanErr)
			return nil, fmt.Errorf("scan aggregated event: %w", scanErr)
		}
		if !found {
			continue
		}
		events = append(events, evt)
	}
	if err := rows.Err(); err != nil {
		observability.RecordError(span, err)
		return nil, fmt.Errorf("iterate aggregated events: %w", err)
	}
	return events, nil
}

// GetClusteringCursor loads the last successful clustering run timestamp.
// The cursor is persisted in impact_service_clustering_state under a fixed key.
// It returns cursor timestamp, found flag, and an error when query fails.
func (r *Repository) GetClusteringCursor(ctx context.Context) (time.Time, bool, error) {
	const q = `SELECT last_run_at FROM impact_service_clustering_state WHERE state_key = $1`
	var lastRunAt time.Time
	if err := r.db.QueryRowContext(ctx, q, clusteringCursorKey).Scan(&lastRunAt); err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("get clustering cursor: %w", err)
	}
	return lastRunAt.UTC(), true, nil
}

// SetClusteringCursor stores the last successful clustering run timestamp.
// The lastRunAt parameter is normalized to UTC and persisted atomically by key.
// It returns an error when persistence fails.
func (r *Repository) SetClusteringCursor(ctx context.Context, lastRunAt time.Time) error {
	const q = `INSERT INTO impact_service_clustering_state (state_key, last_run_at, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT(state_key) DO UPDATE SET
 last_run_at=excluded.last_run_at,
 updated_at=excluded.updated_at`
	now := time.Now().UTC()
	if _, err := r.db.ExecContext(ctx, q, clusteringCursorKey, lastRunAt.UTC(), now); err != nil {
		return fmt.Errorf("set clustering cursor: %w", err)
	}
	return nil
}

// scanEventRow decodes one aggregated event row (including countries, sectors, industries, and article IDs) from either QueryRow or Rows scanners.
// The scanFn parameter is a scanner function matching database/sql Scan signature.
// It returns decoded event, found flag for QueryRow not-found handling, and decode/query error.
func scanEventRow(scanFn func(dest ...any) error) (event.Event, bool, error) {
	var evt event.Event
	var eventTypeRaw, geoClusterRaw, directionRaw string
	var countriesRaw, sectorsRaw, industriesRaw, articleIDsRaw string
	err := scanFn(
		&evt.ID,
		&evt.ClusterKey,
		&eventTypeRaw,
		&geoClusterRaw,
		&countriesRaw,
		&sectorsRaw,
		&industriesRaw,
		&directionRaw,
		&evt.Strength,
		&evt.Confidence,
		&evt.Status,
		&articleIDsRaw,
		&evt.SourceCount,
		&evt.FirstSeenAt,
		&evt.LastSeenAt,
		&evt.CreatedAt,
		&evt.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return event.Event{}, false, nil
		}
		return event.Event{}, false, err
	}
	evt.EventType = extract.EventType(eventTypeRaw)
	evt.GeoCluster = extract.GeoCluster(geoClusterRaw)
	evt.Direction = extract.ImpactDirection(directionRaw)
	if err := json.Unmarshal([]byte(countriesRaw), &evt.Countries); err != nil {
		return event.Event{}, false, fmt.Errorf("decode event countries %s: %w", evt.ID, err)
	}
	if err := json.Unmarshal([]byte(sectorsRaw), &evt.Sectors); err != nil {
		return event.Event{}, false, fmt.Errorf("decode event sectors %s: %w", evt.ID, err)
	}
	if err := json.Unmarshal([]byte(industriesRaw), &evt.Industries); err != nil {
		return event.Event{}, false, fmt.Errorf("decode event industries %s: %w", evt.ID, err)
	}
	if err := json.Unmarshal([]byte(articleIDsRaw), &evt.ArticleIDs); err != nil {
		return event.Event{}, false, fmt.Errorf("decode event article_ids %s: %w", evt.ID, err)
	}
	evt.FirstSeenAt = evt.FirstSeenAt.UTC()
	evt.LastSeenAt = evt.LastSeenAt.UTC()
	evt.CreatedAt = evt.CreatedAt.UTC()
	evt.UpdatedAt = evt.UpdatedAt.UTC()
	return evt, true, nil
}
