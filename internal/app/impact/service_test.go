package impactapp

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"ai-advisor-impact-service/internal/event"
	impactdomain "ai-advisor-impact-service/internal/impact"
	"ai-advisor-impact-service/internal/impact/rules"
	"ai-advisor-impact-service/internal/logging"
	universestore "ai-advisor-impact-service/internal/universe/store"
)

type mockEventSource struct {
	events []event.Event
	err    error
}

func (m mockEventSource) ListEvents(_ context.Context, _ int, _ *time.Time, _ *time.Time) ([]event.Event, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.events, nil
}

type pagedEventSource struct {
	events []event.Event
	call   int
}

func (p *pagedEventSource) ListEvents(_ context.Context, limit int, _ *time.Time, until *time.Time) ([]event.Event, error) {
	p.call++
	filtered := make([]event.Event, 0, len(p.events))
	for _, evt := range p.events {
		if until != nil && evt.LastSeenAt.UTC().After(until.UTC()) {
			continue
		}
		filtered = append(filtered, evt)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left := filtered[i].LastSeenAt.UTC()
		right := filtered[j].LastSeenAt.UTC()
		if left.Equal(right) {
			return filtered[i].ID > filtered[j].ID
		}
		return left.After(right)
	})
	if limit > len(filtered) {
		limit = len(filtered)
	}
	return filtered[:limit], nil
}

type probeAwarePagedEventSource struct {
	events []event.Event
	call   int
}

func (p *probeAwarePagedEventSource) ListEvents(_ context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error) {
	p.call++
	filtered := make([]event.Event, 0, len(p.events))
	for _, evt := range p.events {
		if since != nil && evt.LastSeenAt.UTC().Before(since.UTC()) {
			continue
		}
		if until != nil && evt.LastSeenAt.UTC().After(until.UTC()) {
			continue
		}
		filtered = append(filtered, evt)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left := filtered[i].LastSeenAt.UTC()
		right := filtered[j].LastSeenAt.UTC()
		if left.Equal(right) {
			return filtered[i].ID > filtered[j].ID
		}
		return left.After(right)
	})
	if limit > len(filtered) {
		limit = len(filtered)
	}
	return filtered[:limit], nil
}

type mockUniverseSource struct {
	items []universestore.SecurityProfile
}

func (m mockUniverseSource) GetAll() []universestore.SecurityProfile {
	return m.items
}

type mockImpactSink struct {
	items []impactdomain.EventSecurityImpact
	err   error
}

func (m *mockImpactSink) ReplaceAllEventSecurityImpacts(_ context.Context, impacts []impactdomain.EventSecurityImpact) error {
	if m.err != nil {
		return m.err
	}
	m.items = append(m.items[:0], impacts...)
	return nil
}

func TestServiceRecalculateImpacts_PersistsFilteredResults(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	service, err := NewService(
		mockEventSource{events: []event.Event{
			{
				ID:         "evt1",
				EventType:  "supply_chain",
				Direction:  "negative",
				Strength:   100,
				Confidence: 1,
				LastSeenAt: time.Now().UTC(),
				Status:     event.StatusActive,
			},
		}},
		mockUniverseSource{items: []universestore.SecurityProfile{
			{
				Code:                 "SEC",
				Name:                 "Sec",
				EventTypeSensitivity: map[string]float64{"supply_chain": 1},
				ProfileConfidence:    floatPtr(1),
			},
		}},
		&mockImpactSink{},
		logging.New(),
		Config{
			EventsLimit: 100,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	sink := &mockImpactSink{}
	service.impacts = sink
	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if len(sink.items) == 0 {
		t.Fatal("expected persisted impacts")
	}
}

func TestServiceRecalculateImpacts_DoesNotPersistWhenNoImpacts(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	sink := &mockImpactSink{}
	service, err := NewService(
		mockEventSource{events: []event.Event{
			{
				ID:         "evt1",
				EventType:  "macro",
				Direction:  "positive",
				Strength:   100,
				Confidence: 1,
				LastSeenAt: time.Now().UTC(),
				Status:     event.StatusActive,
			},
		}},
		mockUniverseSource{items: []universestore.SecurityProfile{
			{Code: "SEC", Name: "Sec"},
		}},
		sink,
		logging.New(),
		Config{
			EventsLimit: 100,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if len(sink.items) != 0 {
		t.Fatalf("expected no persisted impacts, got %d", len(sink.items))
	}
}

func TestServiceRecalculateImpacts_ReplacesPreviouslyPersistedImpacts(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	sink := &mockImpactSink{
		items: []impactdomain.EventSecurityImpact{{EventID: "old", SecurityCode: "OLD", RuleVersion: rules.RuleVersion}},
	}
	service, err := NewService(
		mockEventSource{events: []event.Event{
			{
				ID:         "evt1",
				EventType:  "macro",
				Direction:  "positive",
				Strength:   100,
				Confidence: 1,
				LastSeenAt: time.Now().UTC(),
				Status:     event.StatusActive,
			},
		}},
		mockUniverseSource{items: []universestore.SecurityProfile{
			{Code: "SEC", Name: "Sec"},
		}},
		sink,
		logging.New(),
		Config{
			EventsLimit: 100,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if len(sink.items) != 0 {
		t.Fatalf("expected old impacts to be replaced with empty recalculation result, got %d items", len(sink.items))
	}
}

func TestServiceRecalculateImpacts_PropagatesSourceErrors(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	service, err := NewService(
		mockEventSource{err: fmt.Errorf("load failed")},
		mockUniverseSource{},
		&mockImpactSink{},
		logging.New(),
		Config{
			EventsLimit: 100,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if err := service.RecalculateImpacts(context.Background()); err == nil {
		t.Fatal("expected error from event source")
	}
}

func TestServiceRecalculateImpacts_LoadsMultipleEventPagesBeforeReplace(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	now := time.Now().UTC()
	pagedSource := &pagedEventSource{
		events: []event.Event{
			{ID: "evt-c", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now, Status: event.StatusActive},
			{ID: "evt-b", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now, Status: event.StatusActive},
			{ID: "evt-a", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now, Status: event.StatusActive},
			{ID: "evt-0", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
		},
	}
	sink := &mockImpactSink{}
	service, err := NewService(
		pagedSource,
		mockUniverseSource{items: []universestore.SecurityProfile{
			{Code: "SEC", Name: "Sec", EventTypeSensitivity: map[string]float64{"supply_chain": 1}, ProfileConfidence: floatPtr(1)},
		}},
		sink,
		logging.New(),
		Config{
			EventsLimit: 1,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if got := pagedSource.call; got < 2 {
		t.Fatalf("expected multiple page loads, got %d calls", got)
	}
	if len(sink.items) != 4 {
		t.Fatalf("expected impacts from all active events including timestamp ties, got %d", len(sink.items))
	}
}

func TestServiceRecalculateImpacts_ExpandsUntilBoundaryTimestampFullyDrained(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	now := time.Now().UTC()
	source := &probeAwarePagedEventSource{
		events: []event.Event{
			{ID: "t1-2", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now, Status: event.StatusActive},
			{ID: "t1-1", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now, Status: event.StatusActive},
			{ID: "t2-5", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
			{ID: "t2-4", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
			{ID: "t2-3", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
			{ID: "t2-2", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
			{ID: "t2-1", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-time.Minute), Status: event.StatusActive},
			{ID: "t3-1", EventType: "supply_chain", Direction: "negative", Strength: 100, Confidence: 1, LastSeenAt: now.Add(-2 * time.Minute), Status: event.StatusActive},
		},
	}
	sink := &mockImpactSink{}
	service, err := NewService(
		source,
		mockUniverseSource{items: []universestore.SecurityProfile{
			{Code: "SEC", Name: "Sec", EventTypeSensitivity: map[string]float64{"supply_chain": 1}, ProfileConfidence: floatPtr(1)},
		}},
		sink,
		logging.New(),
		Config{
			EventsLimit: 3,
			MinScore:    0,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if len(sink.items) != len(source.events) {
		t.Fatalf("expected one impact per active event, got=%d want=%d", len(sink.items), len(source.events))
	}
}

func TestServiceRecalculateImpacts_HonorsConfigMinScoreBelowRuleSetThreshold(t *testing.T) {
	ruleSet := rules.DefaultRuleSet()
	sink := &mockImpactSink{}
	service, err := NewService(
		mockEventSource{events: []event.Event{
			{
				ID:         "evt-low",
				EventType:  "supply_chain",
				Direction:  "negative",
				Strength:   10,
				Confidence: 1,
				LastSeenAt: time.Now().UTC(),
				Status:     event.StatusActive,
			},
		}},
		mockUniverseSource{items: []universestore.SecurityProfile{
			{
				Code:                 "SEC",
				Name:                 "Sec",
				EventTypeSensitivity: map[string]float64{"supply_chain": 1},
				ProfileConfidence:    floatPtr(1),
			},
		}},
		sink,
		logging.New(),
		Config{
			EventsLimit: 100,
			MinScore:    10,
			RuleSet:     ruleSet,
		},
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	if err := service.RecalculateImpacts(context.Background()); err != nil {
		t.Fatalf("RecalculateImpacts() error = %v", err)
	}
	if len(sink.items) != 1 {
		t.Fatalf("expected low-but-configured score impact to be persisted, got=%d", len(sink.items))
	}
	if sink.items[0].ImpactScore < 10 || sink.items[0].ImpactScore >= 15 {
		t.Fatalf("expected score in [10,15), got=%v", sink.items[0].ImpactScore)
	}
}

func TestToImpactProfile_NormalizesExposureAndSensitivityKeys(t *testing.T) {
	profile := toImpactProfile(universestore.SecurityProfile{
		Code:                 "SEC",
		GeoExposure:          map[string]float64{" US ": 0.4, "us": 0.1, "": 0.7},
		CountryExposure:      map[string]float64{" DE ": 0.2, "de": 0.3},
		EventTypeSensitivity: map[string]float64{" Supply_Chain ": 0.5, "supply_chain": 0.2},
	})

	if got := profile.GeoExposure["us"]; got != 0.5 {
		t.Fatalf("expected normalized geo exposure sum=0.5, got=%v", got)
	}
	if _, exists := profile.GeoExposure[" US "]; exists {
		t.Fatal("expected raw geo exposure key to be absent after normalization")
	}
	if got := profile.CountryExposure["de"]; got != 0.5 {
		t.Fatalf("expected normalized country exposure sum=0.5, got=%v", got)
	}
	if got := profile.EventTypeSensitivity["supply_chain"]; got != 0.7 {
		t.Fatalf("expected normalized event type sensitivity sum=0.7, got=%v", got)
	}
}

func floatPtr(v float64) *float64 { return &v }
