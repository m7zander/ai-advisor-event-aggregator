package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRemovedLegacyRoutesReturn404 ensures removed legacy routes stay unavailable in the public API.
func TestRemovedLegacyRoutesReturn404(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "legacy event securities endpoint", method: http.MethodGet, path: "/api/events/evt-1/securities"},
		{name: "legacy security impacts endpoint", method: http.MethodGet, path: "/api/securities/AAA/impacts"},
		{name: "legacy universe endpoint", method: http.MethodGet, path: "/api/universe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rw := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			mux.ServeHTTP(rw, req)
			if rw.Code != http.StatusNotFound {
				t.Fatalf("expected 404, got %d", rw.Code)
			}
		})
	}
}
