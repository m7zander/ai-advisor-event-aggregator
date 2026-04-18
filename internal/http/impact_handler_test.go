package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestImpactRoutes_RemovedEndpointsReturn404 ensures removed impact routes are not registered anymore.
func TestImpactRoutes_RemovedEndpointsReturn404(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "event securities endpoint removed", method: http.MethodGet, path: "/api/events/evt-1/securities"},
		{name: "security impacts endpoint removed", method: http.MethodGet, path: "/api/securities/AAA/impacts"},
		{name: "event subroute removed for other methods", method: http.MethodPost, path: "/api/events/evt-1/securities"},
		{name: "security subroute removed for other methods", method: http.MethodDelete, path: "/api/securities/AAA/impacts"},
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
