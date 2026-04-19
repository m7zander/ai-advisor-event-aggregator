package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRegister_CurrentRoutesRemainWhileRemovedLegacyRoutesReturn404 verifies the active transport surface excludes deprecated routes.
func TestRegister_CurrentRoutesRemainWhileRemovedLegacyRoutesReturn404(t *testing.T) {
	h := NewHandlerWithExtractionAndClustering(nil, nil, nil, "", nil)
	mux := http.NewServeMux()
	h.Register(mux)

	tests := []struct {
		name   string
		method string
		path   string
		code   int
	}{
		{name: "health", method: http.MethodGet, path: "/health", code: http.StatusOK},
		{name: "api health", method: http.MethodGet, path: "/api/health", code: http.StatusOK},
		{name: "preprocess route registered", method: http.MethodPost, path: "/api/preprocess", code: http.StatusMethodNotAllowed},
		{name: "extract run route registered", method: http.MethodGet, path: "/api/extract/run", code: http.StatusMethodNotAllowed},
		{name: "extract run batch route registered", method: http.MethodGet, path: "/api/extract/run-batch", code: http.StatusMethodNotAllowed},
		{name: "extract result route registered", method: http.MethodPost, path: "/api/extract/result", code: http.StatusMethodNotAllowed},
		{name: "events route registered", method: http.MethodGet, path: "/api/events", code: http.StatusInternalServerError},
		{name: "deprecated event securities route is not registered", method: http.MethodGet, path: "/api/events/e1/securities", code: http.StatusNotFound},
		{name: "deprecated security impacts route is not registered", method: http.MethodGet, path: "/api/securities/ABC/impacts", code: http.StatusNotFound},
		{name: "deprecated rebuild route is not registered", method: http.MethodPost, path: "/api/universe/rebuild", code: http.StatusNotFound},
		{name: "deprecated universe route is not registered", method: http.MethodGet, path: "/api/universe", code: http.StatusNotFound},
		{name: "deprecated universe securities route is not registered", method: http.MethodGet, path: "/api/universe/securities", code: http.StatusNotFound},
		{name: "deprecated universe memberships route is not registered", method: http.MethodGet, path: "/api/universe/memberships", code: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			mux.ServeHTTP(rr, req)
			if rr.Code != tc.code {
				t.Fatalf("expected status %d got %d", tc.code, rr.Code)
			}
		})
	}
}
