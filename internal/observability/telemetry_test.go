package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

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

	inFlightCurrent := &atomic.Int64{}
	_, _, _, _, registration, err := initHTTPMetrics(meterProvider.Meter("test-meter"), inFlightCurrent)
	if err != nil {
		t.Fatalf("initHTTPMetrics() error = %v", err)
	}
	t.Cleanup(func() {
		_ = registration.Unregister()
	})

	inFlightCurrent.Store(4)
	if got := collectInFlightRequests(t, ctx, reader); got != 4 {
		t.Fatalf("in_flight_requests value = %d, want 4", got)
	}

	inFlightCurrent.Store(1)
	if got := collectInFlightRequests(t, ctx, reader); got != 1 {
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

	inFlightCurrent := &atomic.Int64{}
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
	if got := collectInFlightRequests(t, ctx, reader); got != 1 {
		t.Fatalf("in_flight_requests value while request in flight = %d, want 1", got)
	}

	close(release)
	wg.Wait()
	if got := collectInFlightRequests(t, ctx, reader); got != 0 {
		t.Fatalf("in_flight_requests value after request completion = %d, want 0", got)
	}
}

func collectInFlightRequests(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) int64 {
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
			if len(gauge.DataPoints) != 1 {
				t.Fatalf("in_flight_requests datapoint count = %d, want 1", len(gauge.DataPoints))
			}
			return gauge.DataPoints[0].Value
		}
	}

	t.Fatal("in_flight_requests metric not found")
	return 0
}
