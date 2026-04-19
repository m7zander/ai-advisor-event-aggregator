package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestRecoveryMiddleware_PanicAfterFlushAborts(t *testing.T) {
	var stderr bytes.Buffer
	logger := logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	writer := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	h := RecoveryMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("wrapped writer must support http.Flusher")
		}
		flusher.Flush()
		panic("boom after flush")
	}))

	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req = req.WithContext(withRequestContextIDs(req.Context(), "req-flush", trace.TraceID{5}, trace.SpanID{6}))

	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Fatalf("recovered panic=%v, want %v", recovered, http.ErrAbortHandler)
		}
		if !writer.flushed {
			t.Fatal("expected flush to be called")
		}
		entry := parseSingleJSONLogLine(t, stderr.String())
		if entry["reaction"] != "aborted request because response already started" {
			t.Fatalf("reaction=%v, want abort reaction", entry["reaction"])
		}
	}()

	h.ServeHTTP(writer, req)
	t.Fatal("expected panic with http.ErrAbortHandler")
}

func TestRecoveryMiddleware_PanicAfterHijackAborts(t *testing.T) {
	var stderr bytes.Buffer
	logger := logging.NewWithWriters(&bytes.Buffer{}, &stderr)

	writer := newHijackRecorder()
	defer writer.Close()
	h := RecoveryMiddleware(logger, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("wrapped writer must support http.Hijacker")
		}
		if _, _, err := hijacker.Hijack(); err != nil {
			t.Fatalf("hijack failed: %v", err)
		}
		panic("boom after hijack")
	}))

	req := httptest.NewRequest(http.MethodGet, "/hijack", nil)
	req = req.WithContext(withRequestContextIDs(req.Context(), "req-hijack", trace.TraceID{7}, trace.SpanID{8}))

	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Fatalf("recovered panic=%v, want %v", recovered, http.ErrAbortHandler)
		}
		if !writer.hijacked {
			t.Fatal("expected writer to record hijack")
		}
		entry := parseSingleJSONLogLine(t, stderr.String())
		if entry["reaction"] != "aborted request because response already started" {
			t.Fatalf("reaction=%v, want abort reaction", entry["reaction"])
		}
	}()

	h.ServeHTTP(writer, req)
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

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() {
	f.flushed = true
}

type hijackRecorder struct {
	header   http.Header
	hijacked bool
	conn     net.Conn
}

func newHijackRecorder() *hijackRecorder {
	serverConn, clientConn := net.Pipe()
	_ = clientConn.SetDeadline(time.Now().Add(30 * time.Second))
	return &hijackRecorder{
		header: make(http.Header),
		conn:   serverConn,
	}
}

func (h *hijackRecorder) Header() http.Header { return h.header }
func (h *hijackRecorder) Write([]byte) (int, error) {
	return 0, errors.New("write not supported after hijack")
}
func (h *hijackRecorder) WriteHeader(int) {}
func (h *hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return h.conn, bufio.NewReadWriter(bufio.NewReader(h.conn), bufio.NewWriter(h.conn)), nil
}

func (h *hijackRecorder) Close() error {
	if h.conn == nil {
		return nil
	}
	err := h.conn.Close()
	h.conn = nil
	return err
}
