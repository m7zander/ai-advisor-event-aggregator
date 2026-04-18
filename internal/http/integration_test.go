//go:build integration
// +build integration

// Package httpapi contains integration tests for HTTP handlers.
package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/event"
	"ai-advisor-event-aggregator/internal/model"
	"ai-advisor-event-aggregator/internal/preprocess"
	"ai-advisor-event-aggregator/internal/upstream"
)

func TestPreprocessIntegration(t *testing.T) {
	title := "Article title"
	link := "https://example.com/article"
	sourceName := "Example Source"
	description := "Description from upstream"
	bodyContent := "Short content field"
	content := "Dies ist ein valider Satz mit genug Wörtern. Hinweis: Entfernen bitte. Noch ein valider Satz mit ausreichend Inhalt und Kontext."
	excerpt := "Fallback excerpt"

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/articles" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.RawQuery; got != "status=done&decision=relevant&fulltext_status=done&limit=50&offset=0" {
			t.Fatalf("unexpected query: %s", got)
		}
		resp := map[string]any{
			"data": []map[string]any{
				{
					"id":                    153323,
					"title":                 title,
					"link":                  link,
					"source":                sourceName,
					"description":           description,
					"content":               bodyContent,
					"published_at":          "2026-03-28T08:35:02Z",
					"fulltext_excerpt":      excerpt,
					"fulltext_content_text": content,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer upstreamSrv.Close()

	hostPort := strings.TrimPrefix(upstreamSrv.URL, "http://")
	parts := strings.Split(hostPort, ":")
	if len(parts) != 2 {
		t.Fatalf("unexpected host/port format: %s", hostPort)
	}

	client := upstream.NewClient(parts[0], parts[1])
	h := NewHandler(client)
	mux := http.NewServeMux()
	h.Register(mux)

	apiSrv := httptest.NewServer(mux)
	defer apiSrv.Close()

	res, err := http.Get(apiSrv.URL + "/api/preprocess")
	if err != nil {
		t.Fatalf("failed to call preprocess endpoint: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var decoded model.PreprocessResponse
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		t.Fatalf("failed to decode preprocess response: %v", err)
	}

	if len(decoded.Data) != 1 {
		t.Fatalf("expected one article, got %d", len(decoded.Data))
	}

	if decoded.Data[0].ArticleID != 153323 {
		t.Fatalf("unexpected article id: %d", decoded.Data[0].ArticleID)
	}

	if decoded.Data[0].Title != title {
		t.Fatalf("unexpected title: %s", decoded.Data[0].Title)
	}

	if decoded.Data[0].Link != link {
		t.Fatalf("unexpected link: %s", decoded.Data[0].Link)
	}

	if decoded.Data[0].Source != sourceName {
		t.Fatalf("unexpected source: %s", decoded.Data[0].Source)
	}

	if decoded.Data[0].PublishedAt != "2026-03-28T08:35:02Z" {
		t.Fatalf("unexpected published_at: %s", decoded.Data[0].PublishedAt)
	}

	if decoded.Data[0].Text == "" {
		t.Fatal("expected cleaned text to be non-empty")
	}

	if strings.Contains(decoded.Data[0].Text, "Hinweis:") {
		t.Fatal("cleaned text must not contain removed boilerplate marker")
	}

	source := preprocess.BuildInputText(title, &description, &bodyContent, &excerpt, &content)
	want := preprocess.HeadTailExtract(preprocess.RebuildText(preprocess.FilterNoisyFragments(preprocess.RemoveBoilerplateFragments(preprocess.SplitFragments(preprocess.Normalize(source))))))
	if decoded.Data[0].Text != want {
		t.Fatalf("unexpected cleaned text\nwant: %q\ngot:  %q", want, decoded.Data[0].Text)
	}
}

func TestHealthEndpoint(t *testing.T) {
	client := upstream.NewClient("localhost", "9999")
	h := NewHandler(client)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rw := httptest.NewRecorder()

	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if strings.TrimSpace(rw.Body.String()) != "{\"status\":\"ok\"}" {
		t.Fatalf("unexpected health response: %s", rw.Body.String())
	}
}

func TestEventsEndpointIntegration_IncludesIndustries(t *testing.T) {
	clusterSvc := &fakeClusterService{
		events: []event.Event{
			{ID: "evt-1", Industries: []string{}},
			{ID: "evt-2", Industries: []string{" Software ", "software", "BANKS", "banks"}},
		},
	}
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", clusterSvc)
	mux := http.NewServeMux()
	h.Register(mux)
	apiSrv := httptest.NewServer(mux)
	defer apiSrv.Close()

	since := "2026-03-30T00:00:00Z"
	until := "2026-03-30T23:59:59Z"
	res, err := http.Get(apiSrv.URL + "/api/events?limit=2&since=" + since + "&until=" + until)
	if err != nil {
		t.Fatalf("failed to call events endpoint: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var payload struct {
		Limit  int           `json:"limit"`
		Since  *time.Time    `json:"since"`
		Until  *time.Time    `json:"until"`
		Events []event.Event `json:"events"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatalf("decode events response: %v", err)
	}
	if payload.Limit != 2 {
		t.Fatalf("unexpected limit: %d", payload.Limit)
	}
	if payload.Since == nil || payload.Until == nil {
		t.Fatalf("expected since/until in response, got since=%v until=%v", payload.Since, payload.Until)
	}
	if len(payload.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(payload.Events))
	}
	if payload.Events[0].ID != "evt-1" || len(payload.Events[0].Industries) != 0 {
		t.Fatalf("expected first event with empty industries, got %+v", payload.Events[0])
	}
	if payload.Events[1].ID != "evt-2" {
		t.Fatalf("unexpected second event: %+v", payload.Events[1])
	}
	if len(payload.Events[1].Industries) != 4 {
		t.Fatalf("expected industries with original duplicates/casing preserved by transport, got %+v", payload.Events[1].Industries)
	}
}
