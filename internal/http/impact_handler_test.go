package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	appimpact "ai-advisor-impact-service/internal/app/impact"
	impactdomain "ai-advisor-impact-service/internal/impact"
	universestore "ai-advisor-impact-service/internal/universe/store"
)

type fakeImpactService struct {
	eventImpacts    []impactdomain.EventSecurityImpact
	security        universestore.SecurityProfile
	securityImpacts []impactdomain.EventSecurityImpact
	aggregate       appimpact.SecurityImpactAggregate
	eventErr        error
	securityErr     error
	countByEvent    map[string]int
}

func (f *fakeImpactService) GetEventSecurities(_ context.Context, _ string, _ appimpact.EventSecurityFilters) ([]impactdomain.EventSecurityImpact, error) {
	return f.eventImpacts, f.eventErr
}

func (f *fakeImpactService) GetSecurityImpacts(_ context.Context, _ string, _ appimpact.SecurityImpactFilters) (universestore.SecurityProfile, []impactdomain.EventSecurityImpact, appimpact.SecurityImpactAggregate, error) {
	return f.security, f.securityImpacts, f.aggregate, f.securityErr
}

func (f *fakeImpactService) CountByEventIDs(_ context.Context, _ []string) (map[string]int, error) {
	return f.countByEvent, nil
}

type filteredImpactServiceStub struct {
	security  universestore.SecurityProfile
	rawEvents []impactdomain.EventSecurityImpact
	activeIDs map[string]struct{}
}

func (f *filteredImpactServiceStub) GetEventSecurities(_ context.Context, _ string, _ appimpact.EventSecurityFilters) ([]impactdomain.EventSecurityImpact, error) {
	return nil, nil
}

func (f *filteredImpactServiceStub) GetSecurityImpacts(_ context.Context, _ string, _ appimpact.SecurityImpactFilters) (universestore.SecurityProfile, []impactdomain.EventSecurityImpact, appimpact.SecurityImpactAggregate, error) {
	filtered := make([]impactdomain.EventSecurityImpact, 0, len(f.rawEvents))
	top := make([]string, 0, len(f.activeIDs))
	for _, item := range f.rawEvents {
		if _, ok := f.activeIDs[item.EventID]; ok {
			filtered = append(filtered, item)
			top = append(top, item.EventID)
		}
	}
	return f.security, filtered, appimpact.SecurityImpactAggregate{
		Direction:    impactdomain.ImpactDirectionPositive,
		ActiveEvents: len(f.activeIDs),
		TopEventIDs:  top,
	}, nil
}

func (f *filteredImpactServiceStub) CountByEventIDs(_ context.Context, _ []string) (map[string]int, error) {
	return map[string]int{}, nil
}

func TestGetEventSecurities_ValidRequest(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{
		eventImpacts: []impactdomain.EventSecurityImpact{
			{
				SecurityCode:        "AAA",
				SecurityName:        "Alpha",
				ISIN:                "ISIN1",
				ImpactDirection:     impactdomain.ImpactDirectionPositive,
				ImpactScore:         55,
				ImpactConfidence:    0.8,
				GeoMatchScore:       0.5,
				CountryMatchScore:   0.2,
				SectorMatchScore:    0.9,
				EventTypeMatchScore: 0.7,
				ProfileConfidence:   0.8,
				ExplanationCodes:    []string{"rule:impact_rules_v1"},
			},
		},
	})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events/evt-1/securities?limit=10&offset=0&min_score=1&direction=positive", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
}

func TestGetEventSecurities_InvalidEventID(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events/%20/securities", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

func TestGetEventSecurities_NotFoundMappingUsesSentinelError(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{eventErr: appimpact.ErrEventNotFound})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events/evt-404/securities", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.Code)
	}
}

func TestGetEventSecurities_GenericNotFoundTextReturns500(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{eventErr: fmt.Errorf("downstream says: not found")})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events/evt-1/securities", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rw.Code)
	}
}

func TestGetEventSecurities_PaginationAndFiltersValidation(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []string{
		"/api/events/evt-1/securities?limit=0",
		"/api/events/evt-1/securities?offset=-1",
		"/api/events/evt-1/securities?min_score=-1",
		"/api/events/evt-1/securities?min_score=NaN",
		"/api/events/evt-1/securities?min_score=Inf",
		"/api/events/evt-1/securities?direction=neutral",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rw := httptest.NewRecorder()
		mux.ServeHTTP(rw, req)
		if rw.Code != http.StatusBadRequest {
			t.Fatalf("path %s expected 400 got %d", path, rw.Code)
		}
	}
}

func TestGetSecurityImpacts_ValidAndAggregate(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{
		security: universestore.SecurityProfile{Code: "AAA", Name: "Alpha"},
		securityImpacts: []impactdomain.EventSecurityImpact{
			{EventID: "evt-1", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 50, ImpactConfidence: 0.8, ExplanationCodes: []string{"rule:impact_rules_v1"}},
			{EventID: "evt-2", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 20, ImpactConfidence: 0.6, ExplanationCodes: []string{"rule:impact_rules_v1"}},
		},
		aggregate: appimpact.SecurityImpactAggregate{
			PositiveScore: 50,
			NegativeScore: 20,
			NetScore:      30,
			Direction:     impactdomain.ImpactDirectionPositive,
			ActiveEvents:  2,
			TopEventIDs:   []string{"evt-1", "evt-2"},
			TopDrivers:    []string{"rule:impact_rules_v1"},
		},
	})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/securities/AAA/impacts?limit=10&offset=0&min_score=1", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var payload map[string]any
	if err := json.NewDecoder(rw.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	summary, ok := payload["summary"].(map[string]any)
	if !ok || summary["net_impact_score"] != float64(30) {
		t.Fatalf("unexpected summary payload: %#v", payload["summary"])
	}
}

func TestGetSecurityImpacts_UsesServiceEventOrder(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{
		security: universestore.SecurityProfile{Code: "AAA", Name: "Alpha"},
		securityImpacts: []impactdomain.EventSecurityImpact{
			{EventID: "evt-2", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 50, ImpactConfidence: 0.8},
			{EventID: "evt-1", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 10, ImpactConfidence: 0.8},
		},
		aggregate: appimpact.SecurityImpactAggregate{Direction: impactdomain.ImpactDirectionNeutral},
	})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/securities/AAA/impacts", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var payload struct {
		Events []struct {
			EventID string `json:"event_id"`
		} `json:"events"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Events) != 2 || payload.Events[0].EventID != "evt-2" {
		t.Fatalf("expected service ordering preserved, got %+v", payload.Events)
	}
}

func TestGetSecurityImpacts_ActiveOnlyContract(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&filteredImpactServiceStub{
		security: universestore.SecurityProfile{Code: "AAA", Name: "Alpha"},
		rawEvents: []impactdomain.EventSecurityImpact{
			{EventID: "evt-active", ImpactDirection: impactdomain.ImpactDirectionPositive, ImpactScore: 40, ImpactConfidence: 0.8},
			{EventID: "evt-stale", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 15, ImpactConfidence: 0.8},
			{EventID: "evt-closed", ImpactDirection: impactdomain.ImpactDirectionNegative, ImpactScore: 12, ImpactConfidence: 0.8},
		},
		activeIDs: map[string]struct{}{"evt-active": {}},
	})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/securities/AAA/impacts", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var payload struct {
		Summary struct {
			ActiveEventCount float64 `json:"active_event_count"`
		} `json:"summary"`
		Events []struct {
			EventID string `json:"event_id"`
		} `json:"events"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Summary.ActiveEventCount != 1 || len(payload.Events) != 1 || payload.Events[0].EventID != "evt-active" {
		t.Fatalf("expected active-only response payload, got %+v", payload)
	}
}

func TestGetSecurityImpacts_UnknownCodeSentinel(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{securityErr: appimpact.ErrSecurityNotFound})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/securities/AAA/impacts", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.Code)
	}
}

func TestGetSecurityImpacts_GenericNotFoundTextReturns500(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{securityErr: fmt.Errorf("backend not found")})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/securities/AAA/impacts", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rw.Code)
	}
}

func TestGetSecurityImpacts_InvalidSecurityCodePatterns(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{
		security: universestore.SecurityProfile{Code: "ABC", Name: "Alpha"},
	})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []struct {
		name string
		path string
		code int
	}{
		{name: "embedded slash", path: "/api/securities/ABC/DEF/impacts", code: http.StatusBadRequest},
		{name: "space", path: "/api/securities/A%20B/impacts", code: http.StatusBadRequest},
		{name: "punctuation", path: "/api/securities/ABC$/impacts", code: http.StatusBadRequest},
		{name: "valid boundary", path: "/api/securities/ABC.def-123_/impacts", code: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)
			if rw.Code != tc.code {
				t.Fatalf("path %s expected %d got %d", tc.path, tc.code, rw.Code)
			}
		})
	}
}

func TestGetSecurityImpacts_InvalidMinScoreNaNOrInf(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	h.AttachImpactService(&fakeImpactService{})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []string{
		"/api/securities/AAA/impacts?min_score=NaN",
		"/api/securities/AAA/impacts?min_score=Inf",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rw := httptest.NewRecorder()
		mux.ServeHTTP(rw, req)
		if rw.Code != http.StatusBadRequest {
			t.Fatalf("path %s expected 400 got %d", path, rw.Code)
		}
	}
}
