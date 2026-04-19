package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-advisor-event-aggregator/internal/logging"
)

func TestHealth_EncodeFailureLogsStructuredErrorAndReturnsSafeMessage(t *testing.T) {
	var stderr bytes.Buffer
	h := NewHandler(nil)
	h.logger = logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rw := newFailFirstWriteResponseWriter()

	h.health(rw, req)

	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d got %d", http.StatusInternalServerError, rw.Code)
	}

	body := rw.Body.String()
	if body != "failed to encode response\n" {
		t.Fatalf("expected safe error body, got %q", body)
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &entry); err != nil {
		t.Fatalf("decode health error log: %v", err)
	}

	if entry["message"] != "failed to encode health response payload" {
		t.Fatalf("unexpected message: %v", entry["message"])
	}
	if entry["event"] != "http.health.encode_failed" {
		t.Fatalf("unexpected event: %v", entry["event"])
	}
	if entry["failure"] != "health_response_encode_failed" {
		t.Fatalf("unexpected failure: %v", entry["failure"])
	}
	if entry["sanitized_input"] != `{"method":"GET","path":"/api/health"}` {
		t.Fatalf("unexpected sanitized_input: %v", entry["sanitized_input"])
	}
}

type failFirstWriteResponseWriter struct {
	*httptest.ResponseRecorder
	failed bool
}

func newFailFirstWriteResponseWriter() *failFirstWriteResponseWriter {
	return &failFirstWriteResponseWriter{ResponseRecorder: httptest.NewRecorder()}
}

func (w *failFirstWriteResponseWriter) Write(p []byte) (int, error) {
	if !w.failed {
		w.failed = true
		return 0, errors.New("simulated write failure")
	}
	return w.ResponseRecorder.Write(p)
}
