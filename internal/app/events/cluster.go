// Package events provides deterministic clustering orchestration that consolidates extracted results into aggregated events.
package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/extract"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
)

// Repository defines persistence methods required by the clustering use case.
type Repository interface {
	ListSuccessfulExtractionsSince(ctx context.Context, since time.Time) ([]repopkg.SuccessfulExtraction, error)
	GetEventByClusterKey(ctx context.Context, clusterKey string) (event.Event, bool, error)
	GetEventByID(ctx context.Context, eventID string) (event.Event, bool, error)
	UpsertEventByClusterKey(ctx context.Context, evt event.Event) error
	ListEvents(ctx context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error)
	GetClusteringCursor(ctx context.Context) (time.Time, bool, error)
	SetClusteringCursor(ctx context.Context, lastRunAt time.Time) error
}

// ClusterRunResult returns run-level counters and upserted events from one clustering execution.
type ClusterRunResult struct {
	Considered int           `json:"considered"`
	Upserted   int           `json:"upserted"`
	Events     []event.Event `json:"events"`
}

// Service orchestrates deterministic cluster grouping, aggregation, and persistence.
type Service struct {
	repo Repository
}

// NewService constructs a clustering service with required persistence dependency.
// The repo parameter provides extraction-input loading and aggregated event upsert operations.
// It returns a configured service or an error if repo is nil.
func NewService(repo Repository) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repo is nil")
	}
	return &Service{repo: repo}, nil
}

// ClusterNewExtractResults processes successful extraction rows after the provided cursor timestamp.
// The ctx parameter controls repository IO lifecycle and since is an exclusive lower timestamp bound.
// It returns run counters plus updated/inserted events, or an error when loading, aggregation, or persistence fails.
func (s *Service) ClusterNewExtractResults(ctx context.Context, since time.Time, now time.Time) (ClusterRunResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	inputs, err := s.repo.ListSuccessfulExtractionsSince(ctx, since.UTC())
	if err != nil {
		return ClusterRunResult{}, fmt.Errorf("load successful extraction inputs since %s: %w", since.UTC().Format(time.RFC3339), err)
	}

	upserted := make([]event.Event, 0, len(inputs))
	latestProcessedAt := since.UTC()
	for _, item := range inputs {
		if item.FinishedAt.UTC().After(latestProcessedAt) {
			latestProcessedAt = item.FinishedAt.UTC()
		}
		clusterKey := BuildClusterKey(item)
		existing, exists, findErr := s.repo.GetEventByClusterKey(ctx, clusterKey)
		if findErr != nil {
			return ClusterRunResult{}, fmt.Errorf("get event by cluster key %s: %w", clusterKey, findErr)
		}

		var merged event.Event
		if exists {
			merged = MergeEvent(existing, item, now)
		} else {
			created, createErr := NewEventFromExtraction(clusterKey, item, now)
			if createErr != nil {
				return ClusterRunResult{}, fmt.Errorf("build new event for cluster key %s: %w", clusterKey, createErr)
			}
			merged = created
		}
		if err := s.repo.UpsertEventByClusterKey(ctx, merged); err != nil {
			return ClusterRunResult{}, fmt.Errorf("upsert event by cluster key %s: %w", clusterKey, err)
		}
		upserted = append(upserted, merged)
	}
	sort.Slice(upserted, func(i, j int) bool { return upserted[i].ID < upserted[j].ID })

	// Persist cursor every run so scheduler progress has durable state.
	// When rows were processed, advance to the max processed finished_at.
	// When no rows were processed, store run time as last_run watermark.
	cursorToStore := now.UTC()
	if latestProcessedAt.After(since.UTC()) {
		cursorToStore = latestProcessedAt
	}
	if err := s.repo.SetClusteringCursor(ctx, cursorToStore); err != nil {
		return ClusterRunResult{}, fmt.Errorf("store clustering cursor: %w", err)
	}
	return ClusterRunResult{Considered: len(inputs), Upserted: len(upserted), Events: upserted}, nil
}

// ClusterFromStoredCursor processes successful extraction rows after the persisted cursor timestamp.
// The ctx parameter controls repository IO lifecycle and now is used for deterministic status/time updates.
// It returns run counters and updated events, or an error when cursor loading or processing fails.
func (s *Service) ClusterFromStoredCursor(ctx context.Context, now time.Time) (ClusterRunResult, error) {
	since, exists, err := s.repo.GetClusteringCursor(ctx)
	if err != nil {
		return ClusterRunResult{}, fmt.Errorf("load clustering cursor: %w", err)
	}
	if !exists {
		since = time.Unix(0, 0).UTC()
	}
	return s.ClusterNewExtractResults(ctx, since, now)
}

// ListEvents returns persisted events with optional last_seen_at lower/upper bounds.
// The ctx parameter controls repository IO lifecycle, limit bounds result size, and since/until are optional filters.
// It returns sorted events or an error if validation fails or persistence read fails.
func (s *Service) ListEvents(ctx context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	items, err := s.repo.ListEvents(ctx, limit, since, until)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	now := time.Now().UTC()
	for idx := range items {
		// Re-evaluate lifecycle status at read time so stale/closed transitions occur
		// even when no new extraction has arrived for an existing persistent event.
		items[idx].Status = computeEventStatus(items[idx].LastSeenAt, now)
	}
	return items, nil
}

// GetEventByID returns one persisted event with lifecycle status recomputed at read time.
// The ctx parameter controls repository IO lifecycle and eventID identifies a persisted event row.
// It returns the event, found flag, and an error when validation fails or persistence lookup fails.
func (s *Service) GetEventByID(ctx context.Context, eventID string) (event.Event, bool, error) {
	id := strings.TrimSpace(eventID)
	if id == "" {
		return event.Event{}, false, fmt.Errorf("event_id must not be empty")
	}
	item, found, err := s.repo.GetEventByID(ctx, id)
	if err != nil {
		return event.Event{}, false, fmt.Errorf("get event by id: %w", err)
	}
	if !found {
		return event.Event{}, false, nil
	}
	item.Status = computeEventStatus(item.LastSeenAt, time.Now().UTC())
	return item, true, nil
}

// BuildClusterKey builds a stable SHA-256 cluster key from normalized event_type, geo_cluster, countries, sectors, and industries.
// The item parameter supplies one extraction record; normalization lowercases, trims, deduplicates, and sorts slices.
// It returns a lowercase hexadecimal hash string suitable as deterministic cluster identity.
func BuildClusterKey(item repopkg.SuccessfulExtraction) string {
	countries := normalizeStringSlice(item.Countries)
	sectors := normalizeStringSlice(item.Sectors)
	industries := normalizeStringSlice(item.Industries)
	payload := strings.Join([]string{
		normalizeString(string(item.EventType)),
		normalizeString(string(item.GeoCluster)),
		strings.Join(countries, ","),
		strings.Join(sectors, ","),
		strings.Join(industries, ","),
	}, "|")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// NewEventFromExtraction creates a new persistent event from one successful extraction.
// The clusterKey parameter is the deterministic cluster identity, item is one extraction input, and now marks persistence update time.
// It returns a validated event or an error when required fields are missing.
func NewEventFromExtraction(clusterKey string, item repopkg.SuccessfulExtraction, now time.Time) (event.Event, error) {
	if clusterKey == "" {
		return event.Event{}, fmt.Errorf("cluster_key must not be empty")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	countries := normalizeStringSlice(item.Countries)
	sectors := normalizeStringSlice(item.Sectors)
	industries := normalizeStringSlice(item.Industries)
	articleIDs := []int64{item.ArticleID}
	lastSeenAt := item.FinishedAt.UTC()
	evt := event.Event{
		ID:          hashText(clusterKey),
		ClusterKey:  clusterKey,
		EventType:   item.EventType,
		GeoCluster:  item.GeoCluster,
		Countries:   countries,
		Sectors:     sectors,
		Industries:  industries,
		Direction:   item.ImpactDirection,
		Strength:    clampInt(item.ImpactStrength, 0, 100),
		Confidence:  clampFloat(item.Confidence, 0, 1),
		Status:      computeEventStatus(lastSeenAt, now),
		ArticleIDs:  articleIDs,
		SourceCount: 1,
		FirstSeenAt: lastSeenAt,
		LastSeenAt:  lastSeenAt,
		CreatedAt:   now.UTC(),
		UpdatedAt:   now.UTC(),
	}
	if err := evt.Validate(); err != nil {
		return event.Event{}, err
	}
	return evt, nil
}

// MergeEvent incrementally merges one new extraction into an existing persistent event.
// The existing parameter is the currently persisted aggregate, item is the new extraction, and now marks update time.
// It returns a deterministic merged event that preserves idempotency by article_id de-duplication.
func MergeEvent(existing event.Event, item repopkg.SuccessfulExtraction, now time.Time) event.Event {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	merged := existing
	merged.ID = hashText(existing.ClusterKey)
	merged.EventType = existing.EventType
	merged.GeoCluster = existing.GeoCluster

	articleSet := make(map[int64]struct{}, len(existing.ArticleIDs)+1)
	for _, id := range existing.ArticleIDs {
		articleSet[id] = struct{}{}
	}
	_, alreadyPresent := articleSet[item.ArticleID]
	articleSet[item.ArticleID] = struct{}{}
	merged.ArticleIDs = mapInt64KeysSorted(articleSet)
	merged.SourceCount = len(merged.ArticleIDs)

	countrySet := make(map[string]struct{}, len(existing.Countries)+len(item.Countries))
	for _, c := range existing.Countries {
		countrySet[normalizeString(c)] = struct{}{}
	}
	for _, c := range item.Countries {
		norm := normalizeString(c)
		if norm == "" {
			continue
		}
		countrySet[norm] = struct{}{}
	}
	merged.Countries = mapKeysSorted(countrySet)

	sectorSet := make(map[string]struct{}, len(existing.Sectors)+len(item.Sectors))
	for _, s := range existing.Sectors {
		sectorSet[normalizeString(s)] = struct{}{}
	}
	for _, s := range item.Sectors {
		norm := normalizeString(s)
		if norm == "" {
			continue
		}
		sectorSet[norm] = struct{}{}
	}
	merged.Sectors = mapKeysSorted(sectorSet)

	industrySet := make(map[string]struct{}, len(existing.Industries)+len(item.Industries))
	for _, industry := range existing.Industries {
		industrySet[normalizeString(industry)] = struct{}{}
	}
	for _, industry := range item.Industries {
		norm := normalizeString(industry)
		if norm == "" {
			continue
		}
		industrySet[norm] = struct{}{}
	}
	merged.Industries = mapKeysSorted(industrySet)

	finishedAt := item.FinishedAt.UTC()
	if finishedAt.Before(merged.FirstSeenAt) {
		merged.FirstSeenAt = finishedAt
	}
	if finishedAt.After(merged.LastSeenAt) {
		merged.LastSeenAt = finishedAt
	}

	if !alreadyPresent {
		directionCounts := map[extract.ImpactDirection]int{
			existing.Direction:   existing.SourceCount,
			item.ImpactDirection: 1,
		}
		merged.Direction = majorityDirection(directionCounts)

		totalWeight := float64(existing.SourceCount + 1)
		strength := ((float64(existing.Strength) * float64(existing.SourceCount)) + float64(item.ImpactStrength)) / totalWeight
		merged.Strength = clampInt(int(math.Round(strength)), 0, 100)

		// Remove the previously-applied deterministic multi-source bonus before
		// recomputing the running mean to avoid bonus compounding across merges.
		existingBaseConfidence := clampFloat(existing.Confidence-multiSourceBonus(existing.SourceCount), 0, 1)
		confidenceMean := ((existingBaseConfidence * float64(existing.SourceCount)) + item.Confidence) / totalWeight
		confidenceMean += multiSourceBonus(merged.SourceCount)
		merged.Confidence = clampFloat(confidenceMean, 0, 1)
	}

	merged.Status = computeEventStatus(merged.LastSeenAt, now)
	merged.UpdatedAt = now.UTC()
	return merged
}

// multiSourceBonus returns the deterministic confidence bonus for multi-source events.
// The sourceCount parameter is the number of unique article IDs in the aggregate.
// It returns 0.02 when multiple unique sources are present, otherwise zero.
func multiSourceBonus(sourceCount int) float64 {
	if sourceCount > 1 {
		return 0.02
	}
	return 0
}

// computeEventStatus derives deterministic lifecycle status from last-seen recency.
// The lastSeenAt parameter is the latest extraction timestamp and now is the evaluation time.
// It returns active for <2h, stale for 2h-12h, and closed for >12h.
func computeEventStatus(lastSeenAt time.Time, now time.Time) string {
	age := now.UTC().Sub(lastSeenAt.UTC())
	if age < 2*time.Hour {
		return event.StatusActive
	}
	if age <= 12*time.Hour {
		return event.StatusStale
	}
	return event.StatusClosed
}

// normalizeStringSlice normalizes string slices by trimming, lowercasing, removing empties, deduplicating, and sorting.
// The values parameter is the original slice from extraction output.
// It returns a new normalized deterministic slice.
func normalizeStringSlice(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		n := normalizeString(v)
		if n == "" {
			continue
		}
		set[n] = struct{}{}
	}
	out := mapKeysSorted(set)
	return out
}

// normalizeString normalizes a single string by trimming whitespace and converting to lowercase.
// The value parameter is an arbitrary input string.
// It returns a normalized deterministic representation.
func normalizeString(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// majorityDirection deterministically resolves cluster direction using majority vote and tie-breaking rules.
// The counts parameter is the vote count per direction.
// It returns mixed when tied and mixed is among winners; otherwise it returns lexical-min winner.
func majorityDirection(counts map[extract.ImpactDirection]int) extract.ImpactDirection {
	maxCount := 0
	winners := make([]string, 0, len(counts))
	for direction, count := range counts {
		d := string(direction)
		if count > maxCount {
			maxCount = count
			winners = []string{d}
			continue
		}
		if count == maxCount {
			winners = append(winners, d)
		}
	}
	if len(winners) == 1 {
		return extract.ImpactDirection(winners[0])
	}
	for _, winner := range winners {
		if winner == string(extract.ImpactDirectionMixed) {
			return extract.ImpactDirectionMixed
		}
	}
	sort.Strings(winners)
	return extract.ImpactDirection(winners[0])
}

// mapKeysSorted converts a string set map to a sorted slice.
// The in parameter is a set represented as map[string]struct{}.
// It returns sorted keys in ascending lexical order.
func mapKeysSorted(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		if key == "" {
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// mapInt64KeysSorted converts an int64 set map to a sorted ascending slice.
// The in parameter is a set represented as map[int64]struct{}.
// It returns sorted unique IDs.
func mapInt64KeysSorted(in map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// hashText returns a stable SHA-256 lowercase hex digest for deterministic IDs.
// The value parameter is the source text to hash.
// It returns the digest string.
func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// clampInt bounds an integer to the inclusive min/max interval.
// The value parameter is clamped between minValue and maxValue.
// It returns the clamped value.
func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// clampFloat bounds a float64 to the inclusive min/max interval.
// The value parameter is clamped between minValue and maxValue.
// It returns the clamped value.
func clampFloat(value float64, minValue float64, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
