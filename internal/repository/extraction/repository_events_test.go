// Package extractionrepo tests persistence behavior for deterministic aggregated events and clustering input queries.
package extractionrepo

import (
	"context"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/extract"
)

func TestRepository_ListSuccessfulExtractionsSince(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	t0 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if err := repo.UpsertSuccess(context.Background(), 1, "m", t0, validResult(1)); err != nil {
		t.Fatalf("seed old success: %v", err)
	}
	if err := repo.UpsertSuccess(context.Background(), 2, "m", t0.Add(10*time.Minute), validResult(2)); err != nil {
		t.Fatalf("seed new success: %v", err)
	}

	items, err := repo.ListSuccessfulExtractionsSince(context.Background(), t0)
	if err != nil {
		t.Fatalf("list successful extractions since: %v", err)
	}
	if len(items) != 1 || items[0].ArticleID != 2 {
		t.Fatalf("unexpected since items: %+v", items)
	}
	if len(items[0].Industries) != 1 || items[0].Industries[0] != "Software - Application" {
		t.Fatalf("unexpected industries payload: %+v", items[0].Industries)
	}
}

func TestRepository_ListSuccessfulExtractionsSince_IndustriesEmptyArray(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	t0 := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	result := validResult(10)
	result.Industries = []string{}
	if err := repo.UpsertSuccess(context.Background(), 10, "m", t0.Add(2*time.Minute), result); err != nil {
		t.Fatalf("seed success with empty industries: %v", err)
	}

	items, err := repo.ListSuccessfulExtractionsSince(context.Background(), t0)
	if err != nil {
		t.Fatalf("list successful extractions since: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one result row, got %d", len(items))
	}
	if items[0].Industries == nil || len(items[0].Industries) != 0 {
		t.Fatalf("expected empty industries slice, got %+v", items[0].Industries)
	}
}

func TestRepository_UpsertEventByClusterKey_Idempotent(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	evt := event.Event{
		ID:          "event-1",
		ClusterKey:  "cluster-1",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{"software"},
		Direction:   extract.ImpactDirectionPositive,
		Strength:    45,
		Confidence:  0.6,
		Status:      event.StatusActive,
		ArticleIDs:  []int64{101, 102},
		SourceCount: 2,
		FirstSeenAt: now.Add(-1 * time.Hour),
		LastSeenAt:  now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := repo.UpsertEventByClusterKey(context.Background(), evt); err != nil {
		t.Fatalf("upsert event first: %v", err)
	}
	evt.ID = "event-2"
	evt.Confidence = 0.7
	evt.UpdatedAt = now.Add(5 * time.Minute)
	if err := repo.UpsertEventByClusterKey(context.Background(), evt); err != nil {
		t.Fatalf("upsert event second: %v", err)
	}

	stored, found, err := repo.GetEventByClusterKey(context.Background(), "cluster-1")
	if err != nil {
		t.Fatalf("get by cluster key: %v", err)
	}
	if !found {
		t.Fatal("expected event to exist")
	}
	if stored.ID != "event-2" || stored.Confidence != 0.7 {
		t.Fatalf("expected updated row, got %+v", stored)
	}
	if len(stored.Industries) != 1 || stored.Industries[0] != "software" {
		t.Fatalf("expected stored industries, got %+v", stored.Industries)
	}
}

func TestRepository_UpsertAndGetEventByClusterKey_IndustriesRoundTrip(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	evt := event.Event{
		ID:          "event-industries",
		ClusterKey:  "cluster-industries",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{"software", " Software ", "BANKS", "banks", ""},
		Direction:   extract.ImpactDirectionPositive,
		Strength:    40,
		Confidence:  0.65,
		Status:      event.StatusActive,
		ArticleIDs:  []int64{11},
		SourceCount: 1,
		FirstSeenAt: now.Add(-15 * time.Minute),
		LastSeenAt:  now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := repo.UpsertEventByClusterKey(context.Background(), evt); err != nil {
		t.Fatalf("upsert event: %v", err)
	}

	stored, found, err := repo.GetEventByClusterKey(context.Background(), evt.ClusterKey)
	if err != nil {
		t.Fatalf("get event by cluster key: %v", err)
	}
	if !found {
		t.Fatal("expected stored event")
	}
	if len(stored.Industries) != len(evt.Industries) {
		t.Fatalf("unexpected industries length after roundtrip: got=%d want=%d values=%+v", len(stored.Industries), len(evt.Industries), stored.Industries)
	}
	for i := range evt.Industries {
		if stored.Industries[i] != evt.Industries[i] {
			t.Fatalf("unexpected industries value at index %d: got=%q want=%q", i, stored.Industries[i], evt.Industries[i])
		}
	}
}

func TestRepository_ListEvents_WithFilters(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	base := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	older := event.Event{
		ID:          "older",
		ClusterKey:  "k1",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{"software"},
		Direction:   extract.ImpactDirectionNeutral,
		Strength:    40,
		Confidence:  0.5,
		Status:      event.StatusStale,
		ArticleIDs:  []int64{1},
		SourceCount: 1,
		FirstSeenAt: base.Add(-2 * time.Hour),
		LastSeenAt:  base.Add(-1 * time.Hour),
		CreatedAt:   base.Add(-1 * time.Hour),
		UpdatedAt:   base.Add(-1 * time.Hour),
	}
	newer := older
	newer.ID = "newer"
	newer.ClusterKey = "k2"
	newer.Status = event.StatusActive
	newer.LastSeenAt = base
	newer.CreatedAt = base
	newer.UpdatedAt = base
	newer.ArticleIDs = []int64{2}

	_ = repo.UpsertEventByClusterKey(context.Background(), older)
	_ = repo.UpsertEventByClusterKey(context.Background(), newer)

	since := base.Add(-30 * time.Minute)
	latest, err := repo.ListEvents(context.Background(), 5, &since, nil)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(latest) != 1 || latest[0].ID != "newer" {
		t.Fatalf("expected newer only, got %+v", latest)
	}
	if len(latest[0].Industries) != 1 || latest[0].Industries[0] != "software" {
		t.Fatalf("expected industries in listed events, got %+v", latest[0].Industries)
	}
}

func TestRepository_ClusteringCursor(t *testing.T) {
	repo, cleanup := newTestRepository(t)
	defer cleanup()

	ts := time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC)
	if err := repo.SetClusteringCursor(context.Background(), ts); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	got, found, err := repo.GetClusteringCursor(context.Background())
	if err != nil {
		t.Fatalf("get cursor: %v", err)
	}
	if !found || !got.Equal(ts) {
		t.Fatalf("unexpected cursor found=%v value=%s", found, got)
	}
}
