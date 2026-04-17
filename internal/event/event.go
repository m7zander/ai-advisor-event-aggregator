// Package event defines the deterministic aggregated event domain model produced by clustering.
package event

import (
	"fmt"
	"sort"
	"time"

	"ai-advisor-impact-service/internal/extract"
)

const (
	// StatusActive marks events seen within the last two hours.
	StatusActive = "active"
	// StatusStale marks events seen between two and twelve hours ago.
	StatusStale = "stale"
	// StatusClosed marks events not seen for more than twelve hours.
	StatusClosed = "closed"
)

// Event represents one deterministic consolidated event aggregated from one or more extraction results.
type Event struct {
	ID         string
	ClusterKey string

	EventType  extract.EventType
	GeoCluster extract.GeoCluster

	Countries  []string
	Sectors    []string
	Industries []string

	Direction  extract.ImpactDirection
	Strength   int
	Confidence float64
	Status     string

	ArticleIDs []int64

	SourceCount int

	FirstSeenAt time.Time
	LastSeenAt  time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Validate validates structural invariants of an aggregated event before persistence.
// The receiver is the aggregated event to validate.
// It returns an error when required fields are missing or values are out of allowed ranges.
func (e Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("id must not be empty")
	}
	if e.ClusterKey == "" {
		return fmt.Errorf("cluster_key must not be empty")
	}
	if e.EventType == "" {
		return fmt.Errorf("event_type must not be empty")
	}
	if e.GeoCluster == "" {
		return fmt.Errorf("geo_cluster must not be empty")
	}
	if e.Direction == "" {
		return fmt.Errorf("direction must not be empty")
	}
	if e.Status != StatusActive && e.Status != StatusStale && e.Status != StatusClosed {
		return fmt.Errorf("status must be one of active|stale|closed")
	}
	if e.Strength < 0 || e.Strength > 100 {
		return fmt.Errorf("strength must be in range 0..100")
	}
	if e.Confidence < 0 || e.Confidence > 1 {
		return fmt.Errorf("confidence must be in range 0.0..1.0")
	}
	if len(e.ArticleIDs) == 0 {
		return fmt.Errorf("article_ids must not be empty")
	}
	if !sort.SliceIsSorted(e.ArticleIDs, func(i, j int) bool { return e.ArticleIDs[i] < e.ArticleIDs[j] }) {
		return fmt.Errorf("article_ids must be sorted ascending")
	}
	if e.SourceCount != len(e.ArticleIDs) {
		return fmt.Errorf("source_count must match unique article_ids")
	}
	if e.FirstSeenAt.IsZero() || e.LastSeenAt.IsZero() || e.CreatedAt.IsZero() || e.UpdatedAt.IsZero() {
		return fmt.Errorf("timestamps must not be zero")
	}
	if e.LastSeenAt.Before(e.FirstSeenAt) {
		return fmt.Errorf("last_seen_at must be >= first_seen_at")
	}
	return nil
}
