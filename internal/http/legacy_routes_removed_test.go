package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRemovedLegacyRoutes_EventAndSecurityEndpointsReturn404 ensures removed legacy routes are not registered anymore.
func TestRemovedLegacyRoutes_EventAndSecurityEndpointsReturn404(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", &fakeClusterService{})
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "removed legacy event securities endpoint", method: http.MethodGet, path: "/api/events/evt-1/securities"},
		{name: "removed legacy security impacts endpoint", method: http.MethodGet, path: "/api/securities/AAA/impacts"},
		{name: "removed legacy event subroute for other methods", method: http.MethodPost, path: "/api/events/evt-1/securities"},
		{name: "removed legacy security subroute for other methods", method: http.MethodDelete, path: "/api/securities/AAA/impacts"},
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
