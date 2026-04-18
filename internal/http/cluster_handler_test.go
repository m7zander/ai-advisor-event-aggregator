// Package httpapi tests deterministic event HTTP endpoint behavior.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/logging"
)

// fakeClusterService returns deterministic event list outputs for handler tests.
type fakeClusterService struct {
	lastLimit int
	lastSince *time.Time
	lastUntil *time.Time
	events    []event.Event
	err       error
}

// ListEvents captures query arguments and returns a preconfigured set.
// The ctx parameter is accepted for interface compatibility.
// It returns at most limit events in configured order.
func (f *fakeClusterService) ListEvents(_ context.Context, limit int, since *time.Time, until *time.Time) ([]event.Event, error) {
	f.lastLimit = limit
	f.lastSince = since
	f.lastUntil = until
	if f.err != nil {
		return nil, f.err
	}
	if limit > len(f.events) {
		limit = len(f.events)
	}
	return f.events[:limit], nil
}

// TestListEvents_ValidRequest verifies events endpoint returns requested limit and parsed filters.
// It sends valid query parameters and inspects response payload and forwarded arguments.
// It fails if query validation, forwarding, or serialization is incorrect.
func TestListEvents_ValidRequest(t *testing.T) {
	clusterSvc := &fakeClusterService{events: []event.Event{{ID: "e1", Industries: []string{"software"}}, {ID: "e2", Industries: []string{"metals"}}}}
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", clusterSvc)
	mux := http.NewServeMux()
	h.Register(mux)

	since := "2026-03-30T08:00:00Z"
	until := "2026-03-30T12:00:00Z"
	req := httptest.NewRequest(http.MethodGet, "/api/events?limit=1&since="+since+"&until="+until, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var resp struct {
		Limit  int `json:"limit"`
		Events []struct {
			Event                 event.Event `json:"event"`
			AffectedSecurityCount int         `json:"affected_security_count"`
		} `json:"events"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Limit != 1 || len(resp.Events) != 1 || resp.Events[0].Event.ID != "e1" || len(resp.Events[0].Event.Industries) != 1 || resp.Events[0].Event.Industries[0] != "software" {
		t.Fatalf("unexpected response body: %+v", resp)
	}
	if clusterSvc.lastSince == nil || clusterSvc.lastUntil == nil {
		t.Fatal("expected since and until to be passed")
	}
}

// TestListEvents_ResponseContainsIndustriesField verifies /api/events serializes industries on event objects.
// It calls the endpoint and validates JSON contains the Industries key and payload.
// It fails when handler omits industries from event serialization.
func TestListEvents_ResponseContainsIndustriesField(t *testing.T) {
	clusterSvc := &fakeClusterService{events: []event.Event{{ID: "e-industries", Industries: []string{"software", "banks"}}}}
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", clusterSvc)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events?limit=1", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	var payload map[string]any
	if err := json.NewDecoder(rw.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	eventsValue, ok := payload["events"].([]any)
	if !ok || len(eventsValue) != 1 {
		t.Fatalf("unexpected events payload: %#v", payload["events"])
	}
	firstItem, ok := eventsValue[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected event payload type: %#v", eventsValue[0])
	}
	eventPayload, ok := firstItem["event"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested event payload, got %#v", firstItem)
	}
	rawIndustries, ok := eventPayload["Industries"].([]any)
	if !ok {
		t.Fatalf("expected Industries field in event payload, got keys=%v", eventPayload)
	}
	if len(rawIndustries) != 2 || rawIndustries[0] != "software" || rawIndustries[1] != "banks" {
		t.Fatalf("unexpected industries payload: %#v", rawIndustries)
	}
}

// TestListEvents_InvalidLimit verifies invalid limit values are rejected.
// It sends a request with non-positive limit.
// It fails if invalid input is accepted.
func TestListEvents_InvalidLimit(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events?limit=0", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

// TestListEvents_InvalidSince verifies malformed since query values are rejected.
// It sends a request with an invalid timestamp.
// It fails if malformed timestamps are accepted.
func TestListEvents_InvalidSince(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events?since=bad-time", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

// TestListEvents_InvalidUntil verifies malformed until query values are rejected.
// It sends a request with an invalid timestamp.
// It fails if malformed timestamps are accepted.
func TestListEvents_InvalidUntil(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events?until=not-a-time", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

func TestListEvents_ErrorLogContainsMandatoryContractFields(t *testing.T) {
	clusterSvc := &fakeClusterService{err: errors.New("db timeout")}
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", clusterSvc)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	h.logger = logging.NewWithWriters(&stdout, &stderr)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/events?limit=5", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rw.Code)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &logEntry); err != nil {
		t.Fatalf("decode error log: %v", err)
	}
	for _, key := range []string{"failure", "cause", "sanitized_input", "reaction"} {
		if _, ok := logEntry[key]; !ok {
			t.Fatalf("expected mandatory error field %q in log entry: %+v", key, logEntry)
		}
	}
}
