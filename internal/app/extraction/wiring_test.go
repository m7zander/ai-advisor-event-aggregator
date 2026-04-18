// Package extraction tests internal wiring between app orchestration and the real LLM extractor implementation.
package extraction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"ai-advisor-event-aggregator/internal/extract"
	"ai-advisor-event-aggregator/internal/model"
)

// TestNewLLMExtractor_Success verifies valid config builds an Extractor-compatible LLM client.
// It provides required constructor settings with a local base URL.
// It fails if wiring cannot construct the extractor dependency.
func TestNewLLMExtractor_Success(t *testing.T) {
	cfg := LLMConfig{
		APIKey:  "test-key",
		Model:   "gpt-4o-mini",
		BaseURL: "http://127.0.0.1:1",
		Timeout: time.Second,
	}
	got, err := NewLLMExtractor(cfg)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil extractor")
	}
}

// TestNewLLMExtractor_InvalidConfig verifies missing required config is rejected.
// It checks API key, model, and timeout requirements.
// It fails if invalid configuration is accepted.
func TestNewLLMExtractor_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  LLMConfig
	}{
		{name: "api key", cfg: LLMConfig{Model: "gpt-4o-mini", Timeout: time.Second}},
		{name: "model", cfg: LLMConfig{APIKey: "test", Timeout: time.Second}},
		{name: "timeout", cfg: LLMConfig{APIKey: "test", Model: "gpt-4o-mini"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewLLMExtractor(tc.cfg); err == nil {
				t.Fatalf("expected error for invalid %s config", tc.name)
			}
		})
	}
}

// TestFromModelArticle_Success verifies model article adaptation into app-layer article contract.
// It parses PublishedAt and maps all required fields.
// It fails if parsing or mapping is incorrect.
func TestFromModelArticle_Success(t *testing.T) {
	description := "desc"
	in := model.Article{
		ID:          12,
		Title:       "title",
		Link:        "https://example.com",
		Source:      "source",
		PublishedAt: "2026-03-28T08:35:02Z",
		Description: &description,
	}

	got, err := FromModelArticle(in)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if got.ID != int64(in.ID) {
		t.Fatalf("unexpected id mapping: got %d want %d", got.ID, in.ID)
	}
	if got.PublishedAt.IsZero() {
		t.Fatal("expected published_at to be parsed")
	}
	if got.Description == nil || *got.Description != description {
		t.Fatal("expected description pointer mapping")
	}
}

// TestFromModelArticle_InvalidPublishedAt verifies invalid PublishedAt format is rejected.
// It provides a non-RFC3339 published_at string.
// It fails if invalid date parsing is accepted.
func TestFromModelArticle_InvalidPublishedAt(t *testing.T) {
	_, err := FromModelArticle(model.Article{PublishedAt: "not-a-date"})
	if err == nil {
		t.Fatal("expected published_at parse error")
	}
}

// TestRun_EndToEndWithRealLLMExtractor verifies single-article execution through app Run using real llm client implementation.
// It mocks the OpenAI HTTP endpoint and runs full orchestration for one article.
// It fails if preprocess/input-validation/extractor/response-validation wiring is broken.
func TestRun_EndToEndWithRealLLMExtractor(t *testing.T) {
	content := "This article contains enough meaningful words for preprocessing to keep content."
	article := Article{
		ID:          77,
		Title:       "Article title",
		Link:        "https://example.com/a",
		Source:      "example",
		PublishedAt: time.Date(2026, 3, 28, 11, 0, 0, 0, time.UTC),
		Content:     &content,
	}

	result := extract.ExtractResult{
		ArticleID:       article.ID,
		EventType:       extract.EventTypeMacro,
		GeoCluster:      extract.GeoClusterGlobal,
		Countries:       []string{"US"},
		Companies:       []string{"ACME"},
		Sectors:         []string{"industrials"},
		ImpactDirection: extract.ImpactDirectionNeutral,
		ImpactStrength:  10,
		Channels:        []extract.Channel{extract.ChannelRiskSentiment},
		TimeHorizon:     extract.TimeHorizonShort,
		Confidence:      0.5,
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		payload := map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": string(resultJSON)}}},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}))
	defer srv.Close()

	extractor, err := NewLLMExtractor(LLMConfig{
		APIKey:  "test-key",
		Model:   "gpt-4o-mini",
		BaseURL: srv.URL,
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("build extractor: %v", err)
	}

	got, err := Run(context.Background(), article, extractor)
	if err != nil {
		t.Fatalf("expected end-to-end success, got error: %v", err)
	}
	if got.ArticleID != article.ID {
		t.Fatalf("unexpected article id: got %d want %d", got.ArticleID, article.ID)
	}
}
