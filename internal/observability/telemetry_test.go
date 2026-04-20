package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestInFlightRequestsObservableGaugeReportsCurrentValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(ctx)
	})

	inFlightCurrent := newInFlightTracker()
	_, _, _, _, registration, err := initHTTPMetrics(meterProvider.Meter("test-meter"), inFlightCurrent)
	if err != nil {
		t.Fatalf("initHTTPMetrics() error = %v", err)
	}
	t.Cleanup(func() {
		_ = registration.Unregister()
	})

	inFlightCurrent.increment(http.MethodGet, "/events")
	inFlightCurrent.increment(http.MethodGet, "/events")
	inFlightCurrent.increment(http.MethodGet, "/events")
	inFlightCurrent.increment(http.MethodGet, "/events")
	if got := collectInFlightRequestsValue(t, ctx, reader, http.MethodGet, "/events"); got != 4 {
		t.Fatalf("in_flight_requests value = %d, want 4", got)
	}

	inFlightCurrent.decrement(http.MethodGet, "/events")
	inFlightCurrent.decrement(http.MethodGet, "/events")
	inFlightCurrent.decrement(http.MethodGet, "/events")
	if got := collectInFlightRequestsValue(t, ctx, reader, http.MethodGet, "/events"); got != 1 {
		t.Fatalf("in_flight_requests value = %d, want 1", got)
	}
}

func TestHTTPMiddlewareInFlightLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(ctx)
	})

	inFlightCurrent := newInFlightTracker()
	requestCount, requestDurationMS, errorCount, inFlightRequests, registration, err := initHTTPMetrics(meterProvider.Meter("test-meter"), inFlightCurrent)
	if err != nil {
		t.Fatalf("initHTTPMetrics() error = %v", err)
	}
	t.Cleanup(func() {
		_ = registration.Unregister()
	})

	telemetry := &Telemetry{
		requestCount:      requestCount,
		requestDurationMS: requestDurationMS,
		errorCount:        errorCount,
		inFlightRequests:  inFlightRequests,
		inFlightCurrent:   inFlightCurrent,
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	handler := telemetry.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	<-started
	if got := waitForInFlightRequestsValue(t, ctx, reader, http.MethodGet, "/events", 1); got != 1 {
		t.Fatalf("in_flight_requests value while request in flight = %d, want 1", got)
	}

	close(release)
	wg.Wait()
	got, ok := waitForInFlightRequestsPoint(t, ctx, reader, http.MethodGet, "/events", 0, true)
	if !ok || got != 0 {
		t.Fatalf("expected zero-valued in_flight_requests sample after completion, got value=%d exists=%v", got, ok)
	}
	if _, ok := collectInFlightRequestsPoint(t, ctx, reader, http.MethodGet, "/events"); ok {
		t.Fatal("expected in_flight_requests series to be removed after zero sample emission")
	}
}

func TestHTTPMiddlewareInFlightLifecyclePerRouteAndMethod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(ctx)
	})

	inFlightCurrent := newInFlightTracker()
	requestCount, requestDurationMS, errorCount, inFlightRequests, registration, err := initHTTPMetrics(meterProvider.Meter("test-meter"), inFlightCurrent)
	if err != nil {
		t.Fatalf("initHTTPMetrics() error = %v", err)
	}
	t.Cleanup(func() {
		_ = registration.Unregister()
	})

	telemetry := &Telemetry{
		requestCount:      requestCount,
		requestDurationMS: requestDurationMS,
		errorCount:        errorCount,
		inFlightRequests:  inFlightRequests,
		inFlightCurrent:   inFlightCurrent,
	}

	startedFirst := make(chan struct{})
	startedSecond := make(chan struct{})
	release := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once

	handler := telemetry.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events" {
			firstOnce.Do(func() { close(startedFirst) })
		}
		if r.URL.Path == "/health" {
			secondOnce.Do(func() { close(startedSecond) })
		}
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/events", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/health", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	<-startedFirst
	<-startedSecond
	if got := waitForInFlightRequestsValue(t, ctx, reader, http.MethodGet, "/events", 1); got != 1 {
		t.Fatalf("in_flight_requests GET /events = %d, want 1", got)
	}
	if got := waitForInFlightRequestsValue(t, ctx, reader, http.MethodPost, "/health", 1); got != 1 {
		t.Fatalf("in_flight_requests POST /health = %d, want 1", got)
	}

	close(release)
	wg.Wait()
	if got := waitForInFlightRequestsValue(t, ctx, reader, http.MethodGet, "/events", 0); got != 0 {
		t.Fatalf("in_flight_requests GET /events after completion = %d, want 0", got)
	}
	if got := waitForInFlightRequestsValue(t, ctx, reader, http.MethodPost, "/health", 0); got != 0 {
		t.Fatalf("in_flight_requests POST /health after completion = %d, want 0", got)
	}
}

func TestTelemetryShutdownRunsFunctionsInDeclaredOrder(t *testing.T) {
	t.Parallel()

	var order []string
	telemetry := &Telemetry{
		shutdownFns: []func(context.Context) error{
			func(context.Context) error {
				order = append(order, "logs")
				return nil
			},
			func(context.Context) error {
				order = append(order, "metrics")
				return nil
			},
			func(context.Context) error {
				order = append(order, "unregister")
				return nil
			},
			func(context.Context) error {
				order = append(order, "traces")
				return errors.New("trace shutdown failure")
			},
		},
	}

	if err := telemetry.Shutdown(context.Background()); err == nil {
		t.Fatal("Shutdown() error = nil, want joined error")
	}

	want := []string{"logs", "metrics", "unregister", "traces"}
	if len(order) != len(want) {
		t.Fatalf("shutdown order length = %d, want %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("shutdown order[%d] = %q, want %q", i, order[i], want[i])
		}
	}
}

func TestParseOTLPEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpoint     string
		wantHost     string
		wantInsecure bool
		wantPath     string
		wantErr      string
	}{
		{
			name:         "http endpoint",
			endpoint:     "http://collector.internal:4318",
			wantHost:     "collector.internal:4318",
			wantInsecure: true,
			wantPath:     "/",
		},
		{
			name:         "https endpoint",
			endpoint:     "https://collector.example.com:4318",
			wantHost:     "collector.example.com:4318",
			wantInsecure: false,
			wantPath:     "/",
		},
		{
			name:         "path prefix allowed",
			endpoint:     "https://collector.example.com/otel",
			wantHost:     "collector.example.com",
			wantInsecure: false,
			wantPath:     "/otel",
		},
		{
			name:     "signal path rejected",
			endpoint: "https://collector.example.com/v1/traces",
			wantErr:  "signal-specific OTLP path",
		},
		{
			name:     "invalid scheme",
			endpoint: "grpc://collector.internal:4318",
			wantErr:  "unsupported URL scheme",
		},
		{
			name:     "missing host",
			endpoint: "http://",
			wantErr:  "missing host",
		},
		{
			name:     "query not allowed",
			endpoint: "http://collector.internal:4318?debug=true",
			wantErr:  "query string",
		},
		{
			name:     "userinfo not allowed",
			endpoint: "https://user:pass@collector.internal:4318",
			wantErr:  "userinfo",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotHost, gotInsecure, gotPath, err := parseOTLPEndpoint(tt.endpoint)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseOTLPEndpoint() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseOTLPEndpoint() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseOTLPEndpoint() unexpected error = %v", err)
			}
			if gotHost != tt.wantHost {
				t.Fatalf("parseOTLPEndpoint() host = %q, want %q", gotHost, tt.wantHost)
			}
			if gotInsecure != tt.wantInsecure {
				t.Fatalf("parseOTLPEndpoint() insecure = %t, want %t", gotInsecure, tt.wantInsecure)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("parseOTLPEndpoint() path prefix = %q, want %q", gotPath, tt.wantPath)
			}
		})
	}
}

func TestValidateNoInsecureOTLPEnv(t *testing.T) {
	t.Run("allows https without insecure env", func(t *testing.T) {
		if err := validateNoInsecureOTLPEnv(false); err != nil {
			t.Fatalf("validateNoInsecureOTLPEnv(false) unexpected error = %v", err)
		}
	})

	t.Run("rejects insecure env for https", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
		if err := validateNoInsecureOTLPEnv(false); err == nil {
			t.Fatal("validateNoInsecureOTLPEnv(false) error = nil, want conflict error")
		}
	})

	t.Run("rejects invalid boolean in insecure env", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "maybe")
		if err := validateNoInsecureOTLPEnv(false); err == nil {
			t.Fatal("validateNoInsecureOTLPEnv(false) error = nil, want boolean parse error")
		}
	})

	t.Run("allows insecure env for http endpoint", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_LOGS_INSECURE", "true")
		if err := validateNoInsecureOTLPEnv(true); err != nil {
			t.Fatalf("validateNoInsecureOTLPEnv(true) unexpected error = %v", err)
		}
	})

	t.Run("rejects signal endpoint with http scheme for https base endpoint", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://collector.internal:4318/v1/traces")
		if err := validateNoInsecureOTLPEnv(false); err == nil {
			t.Fatal("validateNoInsecureOTLPEnv(false) error = nil, want scheme conflict error")
		}
	})

	t.Run("allows signal endpoint with https scheme for https base endpoint", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://collector.internal:4318/v1/metrics")
		if err := validateNoInsecureOTLPEnv(false); err != nil {
			t.Fatalf("validateNoInsecureOTLPEnv(false) unexpected error = %v", err)
		}
	})
}

func waitForInFlightRequestsValue(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, method string, route string, want int64) int64 {
	t.Helper()

	const attempts = 20
	for i := 0; i < attempts; i++ {
		got := collectInFlightRequestsValue(t, ctx, reader, method, route)
		if got == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return collectInFlightRequestsValue(t, ctx, reader, method, route)
}

func waitForInFlightRequestsPoint(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, method string, route string, want int64, wantExists bool) (int64, bool) {
	t.Helper()

	const attempts = 20
	for i := 0; i < attempts; i++ {
		got, ok := collectInFlightRequestsPoint(t, ctx, reader, method, route)
		if got == want && ok == wantExists {
			return got, ok
		}
		time.Sleep(5 * time.Millisecond)
	}
	return collectInFlightRequestsPoint(t, ctx, reader, method, route)
}

func collectInFlightRequestsValue(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, method string, route string) int64 {
	t.Helper()

	value, ok := collectInFlightRequestsPoint(t, ctx, reader, method, route)
	if !ok {
		return 0
	}
	return value
}

func collectInFlightRequestsPoint(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, method string, route string) (int64, bool) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	for _, scopeMetric := range rm.ScopeMetrics {
		for _, metric := range scopeMetric.Metrics {
			if metric.Name != "in_flight_requests" {
				continue
			}

			gauge, ok := metric.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("in_flight_requests data type = %T, want metricdata.Gauge[int64]", metric.Data)
			}
			for _, point := range gauge.DataPoints {
				pointMethod, methodFound := findAttributeValue(point.Attributes, "http.method")
				pointRoute, routeFound := findAttributeValue(point.Attributes, "http.route")
				if methodFound && routeFound && pointMethod == method && pointRoute == route {
					return point.Value, true
				}
			}
			return 0, false
		}
	}

	return 0, false
}

func findAttributeValue(set attribute.Set, key string) (string, bool) {
	value, ok := set.Value(attribute.Key(key))
	if !ok {
		return "", false
	}
	return value.AsString(), true
}
