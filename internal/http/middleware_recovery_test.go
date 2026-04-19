package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ai-advisor-event-aggregator/internal/logging"
	"go.opentelemetry.io/otel/trace"
)

func TestRecoveryMiddleware_PanicBeforeWriteReturnsSafe500AndLogsContract(t *testing.T) {
	var stderr bytes.Buffer
	logger := logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	h := RecoveryMiddleware(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom before write")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/events?limit=50", nil)
	req = req.WithContext(withRequestContextIDs(req.Context(), "req-before", trace.TraceID{1}, trace.SpanID{2}))
	rw := httptest.NewRecorder()

	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rw.Code, http.StatusInternalServerError)
	}
	if body := rw.Body.String(); !strings.Contains(body, "Internal Server Error") {
		t.Fatalf("body %q does not contain safe 500 message", body)
	}

	entry := parseSingleJSONLogLine(t, stderr.String())
	if entry["failure"] != "handler_panic" {
		t.Fatalf("failure=%v, want handler_panic", entry["failure"])
	}
	if entry["cause"] != "boom before write" {
		t.Fatalf("cause=%v, want boom before write", entry["cause"])
	}
	if entry["reaction"] != "returned safe 500 response" {
		t.Fatalf("reaction=%v, want safe 500 reaction", entry["reaction"])
	}
	if entry["request_id"] != "req-before" {
		t.Fatalf("request_id=%v, want req-before", entry["request_id"])
	}
	if entry["trace_id"] == "" {
		t.Fatal("expected trace_id in panic log")
	}
	if entry["sanitized_input"] != `{"method":"GET","path":"/api/events"}` {
		t.Fatalf("sanitized_input=%v, want method/path only", entry["sanitized_input"])
	}
}

func TestRecoveryMiddleware_PanicAfterWriteAbortsAndLogsContract(t *testing.T) {
	var stderr bytes.Buffer
	logger := logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	h := RecoveryMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		panic("boom after write")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	req = req.WithContext(withRequestContextIDs(req.Context(), "req-after", trace.TraceID{3}, trace.SpanID{4}))
	rw := httptest.NewRecorder()

	defer func() {
		recovered := recover()
		if recovered != http.ErrAbortHandler {
			t.Fatalf("recovered panic=%v, want %v", recovered, http.ErrAbortHandler)
		}

		if rw.Code != http.StatusOK {
			t.Fatalf("status=%d, want %d", rw.Code, http.StatusOK)
		}
		if rw.Body.String() != "partial" {
			t.Fatalf("body=%q, want partial", rw.Body.String())
		}

		entry := parseSingleJSONLogLine(t, stderr.String())
		if entry["failure"] != "handler_panic" {
			t.Fatalf("failure=%v, want handler_panic", entry["failure"])
		}
		if entry["cause"] != "boom after write" {
			t.Fatalf("cause=%v, want boom after write", entry["cause"])
		}
		if entry["reaction"] != "aborted request because response already started" {
			t.Fatalf("reaction=%v, want abort reaction", entry["reaction"])
		}
		if entry["request_id"] != "req-after" {
			t.Fatalf("request_id=%v, want req-after", entry["request_id"])
		}
		if entry["trace_id"] == "" {
			t.Fatal("expected trace_id in panic log")
		}
	}()

	h.ServeHTTP(rw, req)
	t.Fatal("expected panic with http.ErrAbortHandler")
}

func withRequestContextIDs(ctx context.Context, requestID string, traceID trace.TraceID, spanID trace.SpanID) context.Context {
	ctx = logging.WithRequestID(ctx, requestID)
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, spanCtx)
}

func parseSingleJSONLogLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(logs), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one log line, got %d logs=%q", len(lines), logs)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("failed to decode log line: %v", err)
	}
	return entry
}
