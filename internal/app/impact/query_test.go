package impactapp

import (
	"context"
	"fmt"
	"testing"

	"ai-advisor-impact-service/internal/event"
	impactdomain "ai-advisor-impact-service/internal/impact"
	impactrepo "ai-advisor-impact-service/internal/repository/impact"
	universestore "ai-advisor-impact-service/internal/universe/store"
)

type queryTestEvents struct {
	byID map[string]event.Event
}

func (q queryTestEvents) GetEventByID(_ context.Context, eventID string) (event.Event, bool, error) {
	evt, ok := q.byID[eventID]
	return evt, ok, nil
}

type queryTestImpacts struct {
	bySecurity map[string][]impactdomain.EventSecurityImpact
}

func (q queryTestImpacts) ListEventSecurityImpacts(_ context.Context, _ string, _ impactrepo.EventImpactFilter) ([]impactdomain.EventSecurityImpact, error) {
	return nil, nil
}

func (q queryTestImpacts) ListSecurityImpacts(_ context.Context, code string, _ impactrepo.SecurityImpactFilter) ([]impactdomain.EventSecurityImpact, error) {
	return q.bySecurity[code], nil
}

func (q queryTestImpacts) GetImpactsBySecurity(_ context.Context, code string) ([]impactdomain.EventSecurityImpact, error) {
	return q.bySecurity[code], nil
}

func (q queryTestImpacts) CountImpactsByEventIDs(_ context.Context, _ []string) (map[string]int, error) {
	return nil, nil
}

type queryTestUniverse struct {
	byCode map[string]universestore.SecurityProfile
}

func (q queryTestUniverse) GetByCode(code string) (universestore.SecurityProfile, bool) {
	item, ok := q.byCode[code]
	return item, ok
}

func TestComputeSecurityImpactSummary_PositiveNegativeMixedAndNeutral(t *testing.T) {
	svc, err := NewQueryService(
		queryTestEvents{byID: map[string]event.Event{
			"evt-pos":   {ID: "evt-pos", Status: event.StatusActive},
			"evt-neg":   {ID: "evt-neg", Status: event.StatusActive},
			"evt-neu":   {ID: "evt-neu", Status: event.StatusActive},
			"evt-mix":   {ID: "evt-mix", Status: event.StatusActive},
			"evt-stale": {ID: "evt-stale", Status: event.StatusStale},
		}},
		queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
			"AAA": {
				{EventID: "evt-pos", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 45, ExplanationCodes: []string{"sector:energy", "geo:middle_east"}},
				{EventID: "evt-neg", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 10, ExplanationCodes: []string{"sector:airlines", "geo:middle_east"}},
				{EventID: "evt-neu", ImpactDirection: impactdomain.ImpactDirectionNeutral, ImpactScore: 99, ExplanationCodes: []string{"ignored:neutral"}},
				{EventID: "evt-mix", ImpactDirection: impactdomain.ImpactDirectionMixed, ImpactScore: 88, ExplanationCodes: []string{"ignored:mixed"}},
				{EventID: "evt-stale", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 90, ExplanationCodes: []string{"ignored"}},
			},
			"BBB": {
				{EventID: "evt-neg", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 25},
			},
			"CCC": {
				{EventID: "evt-pos", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 15},
				{EventID: "evt-neg", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 15},
			},
		}},
		queryTestUniverse{byCode: map[string]universestore.SecurityProfile{
			"AAA": {Code: "AAA"},
			"BBB": {Code: "BBB"},
			"CCC": {Code: "CCC"},
		}},
	)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}

	positiveSummary, err := svc.ComputeSecurityImpactSummary(context.Background(), "AAA")
	if err != nil {
		t.Fatalf("ComputeSecurityImpactSummary() error = %v", err)
	}
	if positiveSummary.Direction != impactdomain.ImpactDirectionPositive {
		t.Fatalf("expected positive direction, got %s", positiveSummary.Direction)
	}
	if positiveSummary.ActiveEventCount != 4 || positiveSummary.NetScore != 35 {
		t.Fatalf("unexpected positive summary: %+v", positiveSummary)
	}
	if positiveSummary.PositiveScore != 45 || positiveSummary.NegativeScore != 10 {
		t.Fatalf("neutral/mixed should not affect signed sums: %+v", positiveSummary)
	}

	negativeSummary, err := svc.ComputeSecurityImpactSummary(context.Background(), "BBB")
	if err != nil {
		t.Fatalf("ComputeSecurityImpactSummary() error = %v", err)
	}
	if negativeSummary.Direction != impactdomain.ImpactDirectionNegative {
		t.Fatalf("expected negative direction, got %s", negativeSummary.Direction)
	}

	neutralSummary, err := svc.ComputeSecurityImpactSummary(context.Background(), "CCC")
	if err != nil {
		t.Fatalf("ComputeSecurityImpactSummary() error = %v", err)
	}
	if neutralSummary.Direction != impactdomain.ImpactDirectionNeutral {
		t.Fatalf("expected neutral direction, got %s", neutralSummary.Direction)
	}
}

func TestComputeSecurityImpactSummary_ExplanationCodesAndTopEventIDs(t *testing.T) {
	svc, err := NewQueryService(
		queryTestEvents{byID: map[string]event.Event{
			"evt-1": {ID: "evt-1", Status: event.StatusActive},
			"evt-2": {ID: "evt-2", Status: event.StatusActive},
			"evt-3": {ID: "evt-3", Status: event.StatusActive},
			"evt-4": {ID: "evt-4", Status: event.StatusActive},
		}},
		queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
			"AAA": {
				{EventID: "evt-1", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 70, ExplanationCodes: []string{"geo:me", "geo:me", "sector:defense"}},
				{EventID: "evt-2", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 60, ExplanationCodes: []string{"geo:me", "event_type:geopolitical"}},
				{EventID: "evt-3", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 50, ExplanationCodes: []string{"sector:defense"}},
				{EventID: "evt-4", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 20, ExplanationCodes: []string{"country:sa"}},
			},
		}},
		queryTestUniverse{byCode: map[string]universestore.SecurityProfile{"AAA": {Code: "AAA"}}},
	)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	summary, err := svc.ComputeSecurityImpactSummary(context.Background(), "AAA")
	if err != nil {
		t.Fatalf("ComputeSecurityImpactSummary() error = %v", err)
	}
	if got, want := len(summary.TopEventIDs), 3; got != want {
		t.Fatalf("top_event_ids len=%d want=%d", got, want)
	}
	if summary.TopEventIDs[0] != "evt-1" || summary.TopEventIDs[1] != "evt-2" || summary.TopEventIDs[2] != "evt-3" {
		t.Fatalf("unexpected top events: %+v", summary.TopEventIDs)
	}
	if len(summary.TopExplanationCodes) == 0 || summary.TopExplanationCodes[0] != "geo:me" {
		t.Fatalf("unexpected top explanation codes: %+v", summary.TopExplanationCodes)
	}
}

func TestComputeSecurityImpactSummary_TopEventIDsDeduplicated(t *testing.T) {
	svc, err := NewQueryService(
		queryTestEvents{byID: map[string]event.Event{
			"evt-1": {ID: "evt-1", Status: event.StatusActive},
			"evt-2": {ID: "evt-2", Status: event.StatusActive},
			"evt-3": {ID: "evt-3", Status: event.StatusActive},
			"evt-4": {ID: "evt-4", Status: event.StatusActive},
		}},
		queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
			"AAA": {
				{EventID: "evt-1", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 90, RuleVersion: "impact_rules_v1"},
				{EventID: "evt-1", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 85, RuleVersion: "impact_rules_v2"},
				{EventID: "evt-2", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 70},
				{EventID: "evt-3", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 60},
				{EventID: "evt-4", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 50},
			},
		}},
		queryTestUniverse{byCode: map[string]universestore.SecurityProfile{"AAA": {Code: "AAA"}}},
	)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	summary, err := svc.ComputeSecurityImpactSummary(context.Background(), "AAA")
	if err != nil {
		t.Fatalf("ComputeSecurityImpactSummary() error = %v", err)
	}
	if got, want := len(summary.TopEventIDs), 3; got != want {
		t.Fatalf("top_event_ids len=%d want=%d", got, want)
	}
	if summary.TopEventIDs[0] != "evt-1" || summary.TopEventIDs[1] != "evt-2" || summary.TopEventIDs[2] != "evt-3" {
		t.Fatalf("unexpected deduplicated top events: %+v", summary.TopEventIDs)
	}
}

func TestGetSecurityImpacts_IntegrationStyleDataset(t *testing.T) {
	events := queryTestEvents{byID: map[string]event.Event{
		"evt-a": {ID: "evt-a", Status: event.StatusActive},
		"evt-b": {ID: "evt-b", Status: event.StatusActive},
		"evt-c": {ID: "evt-c", Status: event.StatusClosed},
	}}
	impacts := queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
		"AAA": {
			{EventID: "evt-a", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 50, ImpactConfidence: 0.8, ExplanationCodes: []string{"geo:me"}},
			{EventID: "evt-b", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 20, ImpactConfidence: 0.7, ExplanationCodes: []string{"sector:defense"}},
			{EventID: "evt-c", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 90, ImpactConfidence: 0.7, ExplanationCodes: []string{"ignored"}},
		},
		"BBB": {
			{EventID: "evt-a", SecurityCode: "BBB", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 10},
		},
	}}
	universe := queryTestUniverse{byCode: map[string]universestore.SecurityProfile{
		"AAA": {Code: "AAA", Name: "Alpha"},
		"BBB": {Code: "BBB", Name: "Beta"},
	}}
	svc, err := NewQueryService(events, impacts, universe)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	security, eventImpacts, aggregate, err := svc.GetSecurityImpacts(context.Background(), "AAA", SecurityImpactFilters{Limit: 10, Offset: 0, MinScore: 0})
	if err != nil {
		t.Fatalf("GetSecurityImpacts() error = %v", err)
	}
	if security.Code != "AAA" {
		t.Fatalf("unexpected security %+v", security)
	}
	if len(eventImpacts) != 2 || eventImpacts[0].EventID != "evt-a" || eventImpacts[1].EventID != "evt-b" {
		t.Fatalf("expected active-only abs-sorted impacts, got %+v", eventImpacts)
	}
	if aggregate.Direction != impactdomain.ImpactDirectionPositive || aggregate.ActiveEvents != 2 {
		t.Fatalf("unexpected aggregate %+v", aggregate)
	}
	if len(aggregate.TopDrivers) == 0 || len(aggregate.TopEventIDs) == 0 {
		t.Fatalf("expected top drivers and events in aggregate: %+v", aggregate)
	}
}

func TestGetSecurityImpacts_PaginatesAfterActiveFilter(t *testing.T) {
	events := queryTestEvents{byID: map[string]event.Event{
		"evt-stale-high": {ID: "evt-stale-high", Status: event.StatusStale},
		"evt-active-mid": {ID: "evt-active-mid", Status: event.StatusActive},
		"evt-active-low": {ID: "evt-active-low", Status: event.StatusActive},
	}}
	impacts := queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
		"AAA": {
			{EventID: "evt-stale-high", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 90},
			{EventID: "evt-active-mid", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 50},
			{EventID: "evt-active-low", SecurityCode: "AAA", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 10},
		},
	}}
	universe := queryTestUniverse{byCode: map[string]universestore.SecurityProfile{
		"AAA": {Code: "AAA", Name: "Alpha"},
	}}
	svc, err := NewQueryService(events, impacts, universe)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	_, page, _, err := svc.GetSecurityImpacts(context.Background(), "AAA", SecurityImpactFilters{Limit: 1, Offset: 1, MinScore: 0})
	if err != nil {
		t.Fatalf("GetSecurityImpacts() error = %v", err)
	}
	if len(page) != 1 || page[0].EventID != "evt-active-low" {
		t.Fatalf("expected pagination over active-only set, got %+v", page)
	}
}

func TestComputeSecurityImpactSummary_PropagatesLookupError(t *testing.T) {
	svc, err := NewQueryService(
		errorEventLookup{},
		queryTestImpacts{bySecurity: map[string][]impactdomain.EventSecurityImpact{
			"AAA": {{EventID: "evt-x", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 1}},
		}},
		queryTestUniverse{byCode: map[string]universestore.SecurityProfile{"AAA": {Code: "AAA"}}},
	)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	if _, err := svc.ComputeSecurityImpactSummary(context.Background(), "AAA"); err == nil {
		t.Fatal("expected error")
	}
}

type errorEventLookup struct{}

func (errorEventLookup) GetEventByID(context.Context, string) (event.Event, bool, error) {
	return event.Event{}, false, fmt.Errorf("lookup failed")
}
