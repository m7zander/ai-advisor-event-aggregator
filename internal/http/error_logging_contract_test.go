package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-advisor-event-aggregator/internal/logging"
	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
	"ai-advisor-event-aggregator/internal/upstream"
)

func decodeSingleLogEntry(t *testing.T, stderr bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[len(lines)-1]) == "" {
		t.Fatal("expected at least one error log line")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("decode log entry: %v", err)
	}
	return entry
}

func assertContractFields(t *testing.T, entry map[string]any) {
	t.Helper()
	for _, key := range []string{"message", "failure", "cause", "sanitized_input", "reaction"} {
		val, ok := entry[key]
		if !ok {
			t.Fatalf("expected mandatory contract field %q in log entry: %+v", key, entry)
		}
		if key != "cause" && val == "" {
			t.Fatalf("expected non-empty %q in log entry: %+v", key, entry)
		}
	}
}

func newUnavailableUpstreamClient() *upstream.Client {
	return upstream.NewClient("127.0.0.1", "1")
}

func TestPreprocess_FetchFailureLogsErrorContract(t *testing.T) {
	h := NewHandler(newUnavailableUpstreamClient())
	var stderr bytes.Buffer
	h.logger = logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	req := httptest.NewRequest(http.MethodGet, "/api/preprocess", nil)
	rw := httptest.NewRecorder()
	h.preprocess(rw, req)

	if rw.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rw.Code)
	}
	entry := decodeSingleLogEntry(t, stderr)
	assertContractFields(t, entry)
	if entry["failure"] != "preprocess_fetch_articles_failed" {
		t.Fatalf("unexpected failure id: %v", entry["failure"])
	}
}

func TestExtractResult_InvalidArticleIDLogsErrorContract(t *testing.T) {
	h := NewHandlerWithExtraction(nil, nil, &fakeRepo{records: map[int64]repopkg.Record{}}, "gpt-4o-mini")
	var stderr bytes.Buffer
	h.logger = logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	req := httptest.NewRequest(http.MethodGet, "/api/extract/result?article_id=bad", nil)
	rw := httptest.NewRecorder()
	h.extractResult(rw, req)

	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
	entry := decodeSingleLogEntry(t, stderr)
	assertContractFields(t, entry)
	if entry["failure"] != "extract_result_invalid_article_id" {
		t.Fatalf("unexpected failure id: %v", entry["failure"])
	}
}

func TestWriteJSON_EncodeFailureLogsErrorContract(t *testing.T) {
	var stderr bytes.Buffer
	logger := logging.NewWithWriters(&bytes.Buffer{}, &stderr)
	rw := httptest.NewRecorder()
	writeJSON(context.Background(), logger, rw, http.StatusOK, map[string]any{"bad": make(chan int)}, `{"method":"GET","path":"/api/test"}`)

	entry := decodeSingleLogEntry(t, stderr)
	assertContractFields(t, entry)
	if entry["failure"] != "write_json_encode_failed" {
		t.Fatalf("unexpected failure id: %v", entry["failure"])
	}
}

func TestRespondError_LogsContractBeforeWritingHTTPError(t *testing.T) {
	h := NewHandler(nil)
	var stderr bytes.Buffer
	h.logger = logging.NewWithWriters(&bytes.Buffer{}, &stderr)
	rw := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)

	h.respondError(req, rw, "http.test", "http/test", "test message", errors.New("root cause"), logging.ErrorContract{
		Failure:        "test_failure",
		Cause:          "root cause",
		SanitizedInput: `{"method":"GET","path":"/api/health"}`,
		Reaction:       "returned http 500",
	}, "safe", http.StatusInternalServerError)

	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rw.Code)
	}
	entry := decodeSingleLogEntry(t, stderr)
	assertContractFields(t, entry)
}
