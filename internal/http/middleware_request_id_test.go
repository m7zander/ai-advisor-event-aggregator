package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"ai-advisor-impact-service/internal/logging"
)

func TestRequestIDMiddleware_Passthrough(t *testing.T) {
	const incomingID = "req-client-123"

	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := logging.RequestIDFromContext(r.Context()); got != incomingID {
			t.Fatalf("request id in context = %q, want %q", got, incomingID)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set(requestIDHeaderName, incomingID)
	rw := httptest.NewRecorder()

	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusNoContent {
		t.Fatalf("unexpected status code %d", rw.Code)
	}
	if got := rw.Header().Get(requestIDHeaderName); got != incomingID {
		t.Fatalf("response request id = %q, want %q", got, incomingID)
	}
}

func TestRequestIDMiddleware_GeneratesIDWhenMissing(t *testing.T) {
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := logging.RequestIDFromContext(r.Context())
		if len(requestID) != requestIDLength {
			t.Fatalf("generated request id length = %d, want %d", len(requestID), requestIDLength)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rw := httptest.NewRecorder()

	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusNoContent {
		t.Fatalf("unexpected status code %d", rw.Code)
	}
	got := rw.Header().Get(requestIDHeaderName)
	if len(got) != requestIDLength {
		t.Fatalf("response request id length = %d, want %d", len(got), requestIDLength)
	}
}

func TestRequestIDMiddleware_GeneratesIDWhenInvalid(t *testing.T) {
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := logging.RequestIDFromContext(r.Context())
		if len(requestID) != requestIDLength {
			t.Fatalf("generated request id length = %d, want %d", len(requestID), requestIDLength)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set(requestIDHeaderName, "bad\nid")
	rw := httptest.NewRecorder()

	h.ServeHTTP(rw, req)

	if rw.Code != http.StatusNoContent {
		t.Fatalf("unexpected status code %d", rw.Code)
	}
	if got := rw.Header().Get(requestIDHeaderName); got == "bad\nid" {
		t.Fatalf("expected middleware to reject invalid incoming request id")
	}
}
