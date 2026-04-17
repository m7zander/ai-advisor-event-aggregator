// Package events tests deterministic clustering normalization, incremental merge, and orchestration behavior.
package events

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/event"
	"ai-advisor-impact-service/internal/extract"
	repopkg "ai-advisor-impact-service/internal/repository/extraction"
)

// fakeRepo provides deterministic in-memory behavior for clustering service tests.
type fakeRepo struct {
	inputs    []repopkg.SuccessfulExtraction
	events    map[string]event.Event
	cursor    time.Time
	hasCursor bool
}

// ListSuccessfulExtractionsSince returns configured extraction inputs filtered by strict timestamp bound.
// The ctx parameter is accepted for interface compatibility and since is applied as exclusive lower bound.
// It returns deterministic in-memory test rows.
func (f *fakeRepo) ListSuccessfulExtractionsSince(_ context.Context, since time.Time) ([]repopkg.SuccessfulExtraction, error) {
	out := make([]repopkg.SuccessfulExtraction, 0)
	for _, item := range f.inputs {
		if item.FinishedAt.After(since) {
			out = append(out, item)
		}
	}
	return out, nil
}

// GetEventByClusterKey returns one stored event by cluster key.
// The ctx parameter is accepted for interface compatibility and clusterKey identifies the lookup key.
// It returns stored event, found flag, and nil error.
func (f *fakeRepo) GetEventByClusterKey(_ context.Context, clusterKey string) (event.Event, bool, error) {
	evt, ok := f.events[clusterKey]
	return evt, ok, nil
}

// GetEventByID returns one stored event by event id.
// The ctx parameter is accepted for interface compatibility and eventID identifies lookup key.
// It returns stored event, found flag, and nil error.
func (f *fakeRepo) GetEventByID(_ context.Context, eventID string) (event.Event, bool, error) {
	for _, evt := range f.events {
		if evt.ID == eventID {
			return evt, true, nil
		}
	}
	return event.Event{}, false, nil
}

// UpsertEventByClusterKey stores events by cluster key.
// The ctx parameter is accepted for interface compatibility and evt is persisted in-memory.
// It returns nil for successful in-memory upsert.
func (f *fakeRepo) UpsertEventByClusterKey(_ context.Context, evt event.Event) error {
	if f.events == nil {
		f.events = map[string]event.Event{}
	}
	f.events[evt.ClusterKey] = evt
	return nil
}

// ListEvents returns in-memory events ordered by last_seen_at descending for service compatibility.
// The ctx parameter is accepted for interface compatibility and limit bounds the result length.
// It returns at most limit rows.
func (f *fakeRepo) ListEvents(_ context.Context, limit int, _ *time.Time, _ *time.Time) ([]event.Event, error) {
	out := make([]event.Event, 0, len(f.events))
	for _, evt := range f.events {
		out = append(out, evt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeenAt.After(out[j].LastSeenAt) })
	if limit < len(out) {
		return out[:limit], nil
	}
	return out, nil
}

// GetClusteringCursor returns the configured cursor.
// The ctx parameter is accepted for interface compatibility.
// It returns cursor timestamp, found flag, and nil error.
func (f *fakeRepo) GetClusteringCursor(_ context.Context) (time.Time, bool, error) {
	return f.cursor, f.hasCursor, nil
}

// SetClusteringCursor stores the cursor in-memory.
// The ctx parameter is accepted for interface compatibility and lastRunAt is persisted.
// It returns nil for successful in-memory update.
func (f *fakeRepo) SetClusteringCursor(_ context.Context, lastRunAt time.Time) error {
	f.cursor = lastRunAt
	f.hasCursor = true
	return nil
}

// TestBuildClusterKey_Normalization verifies normalized-equivalent records produce the same cluster key.
// It compares inputs with differing case/order/spacing and expects equal deterministic hash output.
// It fails when normalization behavior regresses.
func TestBuildClusterKey_Normalization(t *testing.T) {
	a := repopkg.SuccessfulExtraction{EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{" US", "us", "", "ca"}, Sectors: []string{"energy", " Energy "}, Industries: []string{"Software", " software "}}
	b := repopkg.SuccessfulExtraction{EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"ca", "US"}, Sectors: []string{"energy"}, Industries: []string{"software"}}
	if BuildClusterKey(a) != BuildClusterKey(b) {
		t.Fatal("expected equal cluster keys for normalized equivalent fields")
	}
}

func TestBuildClusterKey_IndustriesAffectIdentity(t *testing.T) {
	base := repopkg.SuccessfulExtraction{EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"us"}, Sectors: []string{"energy"}, Industries: []string{"software"}}
	changed := repopkg.SuccessfulExtraction{EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"us"}, Sectors: []string{"energy"}, Industries: []string{"semiconductors"}}
	if BuildClusterKey(base) == BuildClusterKey(changed) {
		t.Fatal("expected differing cluster keys when industries differ")
	}
}

// TestBuildClusterKey_EmptyIndustriesAffectIdentity verifies empty and non-empty industry lists do not collide.
// It compares otherwise-identical inputs where one has no industries.
// It fails when cluster identity ignores industry presence.
func TestBuildClusterKey_EmptyIndustriesAffectIdentity(t *testing.T) {
	base := repopkg.SuccessfulExtraction{
		EventType:  extract.EventTypeMacro,
		GeoCluster: extract.GeoClusterGlobal,
		Countries:  []string{"us"},
		Sectors:    []string{"energy"},
		Industries: []string{},
	}
	changed := base
	changed.Industries = []string{" Software ", "software", "SOFTWARE"}
	if BuildClusterKey(base) == BuildClusterKey(changed) {
		t.Fatal("expected differing cluster keys when one industries list is empty and the other is not")
	}
}

// TestMergeEvent_IndustriesUnionDedupeSort verifies merge forms deterministic industries union with normalization.
// It merges mixed-case and whitespace-padded industries including duplicates across existing and incoming values.
// It fails when merge does not normalize, deduplicate, and sort industries deterministically.
func TestMergeEvent_IndustriesUnionDedupeSort(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	existing := event.Event{
		ID:          "evt-1",
		ClusterKey:  "cluster-1",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{" software ", "Banks", "banks", ""},
		Direction:   extract.ImpactDirectionPositive,
		Strength:    55,
		Confidence:  0.6,
		Status:      event.StatusActive,
		ArticleIDs:  []int64{1},
		SourceCount: 1,
		FirstSeenAt: now.Add(-20 * time.Minute),
		LastSeenAt:  now.Add(-10 * time.Minute),
		CreatedAt:   now.Add(-20 * time.Minute),
		UpdatedAt:   now.Add(-10 * time.Minute),
	}
	incoming := repopkg.SuccessfulExtraction{
		ArticleID:       2,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Industries:      []string{"  software", "Semiconductors", "semiconductors", " ", "\tBANKS\t"},
		ImpactDirection: extract.ImpactDirectionNegative,
		ImpactStrength:  40,
		Confidence:      0.7,
		FinishedAt:      now,
	}

	merged := MergeEvent(existing, incoming, now)
	want := []string{"banks", "semiconductors", "software"}
	if len(merged.Industries) != len(want) {
		t.Fatalf("unexpected industries length: got=%d want=%d values=%+v", len(merged.Industries), len(want), merged.Industries)
	}
	for i := range want {
		if merged.Industries[i] != want[i] {
			t.Fatalf("unexpected industries union order/content: got=%+v want=%+v", merged.Industries, want)
		}
	}
}

func TestServiceGetEventByID_RecomputesLifecycleStatusAtReadTime(t *testing.T) {
	now := time.Now().UTC()
	stored := event.Event{
		ID:          "evt-1",
		ClusterKey:  "cluster-1",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Direction:   extract.ImpactDirectionPositive,
		Strength:    10,
		Confidence:  0.9,
		Status:      event.StatusActive,
		LastSeenAt:  now.Add(-13 * time.Hour),
		FirstSeenAt: now.Add(-20 * time.Hour),
		CreatedAt:   now.Add(-20 * time.Hour),
		UpdatedAt:   now.Add(-13 * time.Hour),
	}
	svc, err := NewService(&fakeRepo{
		events: map[string]event.Event{stored.ClusterKey: stored},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	got, found, err := svc.GetEventByID(context.Background(), stored.ID)
	if err != nil {
		t.Fatalf("GetEventByID() error = %v", err)
	}
	if !found {
		t.Fatal("expected event to be found")
	}
	if got.Status != event.StatusClosed {
		t.Fatalf("expected recomputed status=%s, got=%s", event.StatusClosed, got.Status)
	}
}

// TestService_IncrementalUpdateSinglePersistentEvent verifies multiple new rows in one cluster persist as a single event.
// It runs incremental clustering on two inputs that share one cluster key.
// It fails if repository persistence creates fragmented event rows.
func TestService_IncrementalUpdateSinglePersistentEvent(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	items := []repopkg.SuccessfulExtraction{
		{ArticleID: 1, EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"US"}, Sectors: []string{"energy"}, Industries: []string{"software"}, ImpactDirection: extract.ImpactDirectionPositive, ImpactStrength: 40, Confidence: 0.6, FinishedAt: now.Add(-10 * time.Minute)},
		{ArticleID: 2, EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"us"}, Sectors: []string{"Energy"}, Industries: []string{"Software"}, ImpactDirection: extract.ImpactDirectionNegative, ImpactStrength: 60, Confidence: 0.8, FinishedAt: now.Add(-5 * time.Minute)},
	}
	repo := &fakeRepo{inputs: items, events: map[string]event.Event{}}
	svc, _ := NewService(repo)
	result, err := svc.ClusterNewExtractResults(context.Background(), now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("cluster incremental: %v", err)
	}
	if result.Upserted != 2 {
		t.Fatalf("expected two incremental upserts, got %d", result.Upserted)
	}
	if len(repo.events) != 1 {
		t.Fatalf("expected one persistent event by cluster key, got %d", len(repo.events))
	}
}

// TestService_NoDuplicationOnRepeatedRuns verifies duplicate processing does not duplicate article IDs.
// It intentionally reruns the same since-range inputs twice.
// It fails if merged persistent event state is not idempotent.
func TestService_NoDuplicationOnRepeatedRuns(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	item := repopkg.SuccessfulExtraction{ArticleID: 7, EventType: extract.EventTypeMacro, GeoCluster: extract.GeoClusterGlobal, Countries: []string{"us"}, Sectors: []string{"energy"}, Industries: []string{"software"}, ImpactDirection: extract.ImpactDirectionPositive, ImpactStrength: 40, Confidence: 0.5, FinishedAt: now.Add(-10 * time.Minute)}
	repo := &fakeRepo{inputs: []repopkg.SuccessfulExtraction{item}, events: map[string]event.Event{}}
	svc, _ := NewService(repo)

	if _, err := svc.ClusterNewExtractResults(context.Background(), now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := svc.ClusterNewExtractResults(context.Background(), now.Add(-1*time.Hour), now); err != nil {
		t.Fatalf("second run: %v", err)
	}
	for _, evt := range repo.events {
		if len(evt.ArticleIDs) != 1 {
			t.Fatalf("expected unique article ids after rerun, got %+v", evt.ArticleIDs)
		}
	}
}

// TestService_ClusterFromStoredCursor_Idempotent verifies cursor-based scheduling avoids reprocessing old inputs.
// It runs once without an existing cursor and then again after cursor advancement.
// It fails if second run still considers previously processed extraction rows.
func TestService_ClusterFromStoredCursor_Idempotent(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	item := repopkg.SuccessfulExtraction{
		ArticleID:       55,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"us"},
		Sectors:         []string{"energy"},
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  41,
		Confidence:      0.6,
		FinishedAt:      now.Add(-5 * time.Minute),
	}
	repo := &fakeRepo{inputs: []repopkg.SuccessfulExtraction{item}, events: map[string]event.Event{}}
	svc, _ := NewService(repo)

	first, err := svc.ClusterFromStoredCursor(context.Background(), now)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := svc.ClusterFromStoredCursor(context.Background(), now.Add(1*time.Minute))
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if first.Considered != 1 || second.Considered != 0 {
		t.Fatalf("expected cursor-based idempotency, got first=%d second=%d", first.Considered, second.Considered)
	}
}

// TestService_ClusterNewExtractResults_CursorTracksProcessedTime verifies cursor advancement uses processed extraction timestamps.
// It feeds one old extraction while passing a much later run clock value.
// It fails if cursor is advanced to scheduler wall-clock time instead of extraction finished_at.
func TestService_ClusterNewExtractResults_CursorTracksProcessedTime(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	finishedAt := now.Add(-20 * time.Minute)
	item := repopkg.SuccessfulExtraction{
		ArticleID:       99,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"us"},
		Sectors:         []string{"energy"},
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  33,
		Confidence:      0.6,
		FinishedAt:      finishedAt,
	}
	repo := &fakeRepo{inputs: []repopkg.SuccessfulExtraction{item}, events: map[string]event.Event{}}
	svc, _ := NewService(repo)

	_, err := svc.ClusterNewExtractResults(context.Background(), now.Add(-2*time.Hour), now)
	if err != nil {
		t.Fatalf("cluster run: %v", err)
	}
	if !repo.cursor.Equal(finishedAt) {
		t.Fatalf("expected cursor=%s, got %s", finishedAt, repo.cursor)
	}
}

// TestService_ClusterNewExtractResults_StoresRunWatermarkWhenNoInputs verifies cursor persistence on empty runs.
// It runs clustering with no eligible extraction rows.
// It fails if cursor is not stored to the run timestamp.
func TestService_ClusterNewExtractResults_StoresRunWatermarkWhenNoInputs(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{inputs: []repopkg.SuccessfulExtraction{}, events: map[string]event.Event{}}
	svc, _ := NewService(repo)

	result, err := svc.ClusterNewExtractResults(context.Background(), now.Add(-1*time.Hour), now)
	if err != nil {
		t.Fatalf("cluster run: %v", err)
	}
	if result.Considered != 0 || result.Upserted != 0 {
		t.Fatalf("unexpected run counts: %+v", result)
	}
	if !repo.cursor.Equal(now) {
		t.Fatalf("expected cursor=%s, got %s", now, repo.cursor)
	}
}

// TestNewEventFromExtraction_StableEventIDAcrossTimes verifies event identity does not depend on run time.
// It builds the same semantic event at different wall-clock times.
// It fails if event IDs differ across those runs.
func TestNewEventFromExtraction_StableEventIDAcrossTimes(t *testing.T) {
	item := repopkg.SuccessfulExtraction{
		ArticleID:       88,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"us"},
		Sectors:         []string{"energy"},
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  50,
		Confidence:      0.6,
		FinishedAt:      time.Date(2026, 3, 30, 8, 0, 0, 0, time.UTC),
	}
	key := BuildClusterKey(item)
	a, err := NewEventFromExtraction(key, item, time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build event a: %v", err)
	}
	b, err := NewEventFromExtraction(key, item, time.Date(2026, 3, 30, 18, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build event b: %v", err)
	}
	if a.ID != b.ID {
		t.Fatalf("expected stable event id across time, got %s vs %s", a.ID, b.ID)
	}
}

// TestMergeEvent_ArticleAccumulationAndAggregation verifies incremental merge updates aggregate fields deterministically.
// It merges one new extraction into an existing event and checks IDs, counts, and recomputed metrics.
// It fails when accumulation or weighted recomputation semantics regress.
func TestMergeEvent_ArticleAccumulationAndAggregation(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	existing := event.Event{
		ID:          "id",
		ClusterKey:  "k",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{"software"},
		Direction:   extract.ImpactDirectionPositive,
		Strength:    30,
		Confidence:  0.4,
		Status:      event.StatusActive,
		ArticleIDs:  []int64{1},
		SourceCount: 1,
		FirstSeenAt: now.Add(-30 * time.Minute),
		LastSeenAt:  now.Add(-30 * time.Minute),
		CreatedAt:   now.Add(-30 * time.Minute),
		UpdatedAt:   now.Add(-30 * time.Minute),
	}
	incoming := repopkg.SuccessfulExtraction{ArticleID: 2, Countries: []string{"ca"}, Sectors: []string{"materials"}, Industries: []string{"metals"}, ImpactDirection: extract.ImpactDirectionNegative, ImpactStrength: 70, Confidence: 0.8, FinishedAt: now.Add(-5 * time.Minute)}
	merged := MergeEvent(existing, incoming, now)

	if got, want := merged.ArticleIDs, []int64{1, 2}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected article accumulation: %+v", got)
	}
	if merged.SourceCount != 2 {
		t.Fatalf("expected source_count 2, got %d", merged.SourceCount)
	}
	if got, want := merged.Industries, []string{"metals", "software"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected industries accumulation: %+v", got)
	}
	if merged.Strength != 50 {
		t.Fatalf("expected running average strength 50, got %d", merged.Strength)
	}
	if merged.Confidence <= 0.59 || merged.Confidence > 1.0 {
		t.Fatalf("unexpected confidence recompute: %f", merged.Confidence)
	}
}

// TestMergeEvent_ConfidenceBonusDoesNotCompound verifies deterministic confidence bonus is not repeatedly stacked.
// It merges a third unique article into a two-source event that already has bonus-adjusted confidence.
// It fails if merge logic compounds bonus on top of previously bonus-inflated confidence.
func TestMergeEvent_ConfidenceBonusDoesNotCompound(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	existing := event.Event{
		ID:          "id",
		ClusterKey:  "k",
		EventType:   extract.EventTypeMacro,
		GeoCluster:  extract.GeoClusterGlobal,
		Countries:   []string{"us"},
		Sectors:     []string{"energy"},
		Industries:  []string{"software"},
		Direction:   extract.ImpactDirectionPositive,
		Strength:    40,
		Confidence:  0.62, // base mean 0.60 plus deterministic bonus 0.02.
		Status:      event.StatusActive,
		ArticleIDs:  []int64{1, 2},
		SourceCount: 2,
		FirstSeenAt: now.Add(-1 * time.Hour),
		LastSeenAt:  now.Add(-20 * time.Minute),
		CreatedAt:   now.Add(-1 * time.Hour),
		UpdatedAt:   now.Add(-20 * time.Minute),
	}
	incoming := repopkg.SuccessfulExtraction{
		ArticleID:       3,
		ImpactDirection: extract.ImpactDirectionPositive,
		ImpactStrength:  50,
		Confidence:      0.90,
		FinishedAt:      now.Add(-5 * time.Minute),
	}

	merged := MergeEvent(existing, incoming, now)

	// expected base mean = (0.60*2 + 0.90)/3 = 0.70, then one bonus +0.02 => 0.72
	if math.Abs(merged.Confidence-0.72) > 0.000001 {
		t.Fatalf("expected confidence 0.72, got %f", merged.Confidence)
	}
}

// TestComputeEventStatus_Lifecycle verifies active/stale/closed transitions by recency thresholds.
// It evaluates three timestamps at representative boundary ranges.
// It fails if lifecycle state mapping deviates from deterministic policy.
func TestComputeEventStatus_Lifecycle(t *testing.T) {
	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	if got := computeEventStatus(now.Add(-90*time.Minute), now); got != event.StatusActive {
		t.Fatalf("expected active, got %s", got)
	}
	if got := computeEventStatus(now.Add(-4*time.Hour), now); got != event.StatusStale {
		t.Fatalf("expected stale, got %s", got)
	}
	if got := computeEventStatus(now.Add(-13*time.Hour), now); got != event.StatusClosed {
		t.Fatalf("expected closed, got %s", got)
	}
}
