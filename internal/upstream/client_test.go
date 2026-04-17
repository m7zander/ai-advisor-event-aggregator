// Package upstream provides a typed HTTP client for the News Intake Service.
package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestListEligibleArticles verifies request query parameters and response decoding.
func TestListEligibleArticles(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("status") != "done" || q.Get("decision") != "relevant" || q.Get("fulltext_status") != "done" || q.Get("limit") != "25" || q.Get("offset") != "50" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":123,"title":"t","link":"l","source":"s","published_at":"2026-03-29T00:00:00Z"}]}`))
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	host := strings.Split(u.Host, ":")[0]
	port := strings.Split(u.Host, ":")[1]
	client := NewClient(host, port)

	resp, err := client.ListEligibleArticles(context.Background(), 25, 50)
	if err != nil {
		t.Fatalf("ListEligibleArticles() error = %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0].ID != 123 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

// TestListEligibleArticlesHTTPError verifies non-200 upstream status handling.
func TestListEligibleArticlesHTTPError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad", http.StatusBadGateway)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	host := strings.Split(u.Host, ":")[0]
	port := strings.Split(u.Host, ":")[1]
	client := NewClient(host, port)

	if _, err := client.ListEligibleArticles(context.Background(), 10, 0); err == nil {
		t.Fatal("ListEligibleArticles() error=nil, want non-nil")
	}
}

// TestListEligibleArticlesMalformedPayload verifies decode errors are surfaced.
func TestListEligibleArticlesMalformedPayload(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":`))
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)
	host := strings.Split(u.Host, ":")[0]
	port := strings.Split(u.Host, ":")[1]
	client := NewClient(host, port)

	if _, err := client.ListEligibleArticles(context.Background(), 10, 0); err == nil {
		t.Fatal("ListEligibleArticles() error=nil, want non-nil")
	}
}
