package observability

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Telemetry struct {
	requestCount      metric.Int64Counter
	requestDurationMS metric.Float64Histogram
	errorCount        metric.Int64Counter
	inFlightRequests  metric.Int64ObservableGauge
	inFlightCurrent   *inFlightTracker
	shutdownFns       []func(context.Context) error
}

type inFlightKey struct {
	method string
	route  string
}

type inFlightTracker struct {
	mu              sync.Mutex
	counts          map[inFlightKey]int64
	pendingZeroEmit map[inFlightKey]struct{}
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{
		counts:          make(map[inFlightKey]int64),
		pendingZeroEmit: make(map[inFlightKey]struct{}),
	}
}

func (t *inFlightTracker) increment(method string, route string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := inFlightKey{method: method, route: route}
	t.counts[key]++
	delete(t.pendingZeroEmit, key)
}

func (t *inFlightTracker) decrement(method string, route string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := inFlightKey{method: method, route: route}
	current := t.counts[key]
	if current <= 1 {
		t.counts[key] = 0
		t.pendingZeroEmit[key] = struct{}{}
		return
	}
	t.counts[key] = current - 1
}

func (t *inFlightTracker) snapshot() map[inFlightKey]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	cloned := make(map[inFlightKey]int64, len(t.counts))
	for key, value := range t.counts {
		cloned[key] = value
		if value == 0 {
			if _, shouldDrop := t.pendingZeroEmit[key]; shouldDrop {
				delete(t.counts, key)
				delete(t.pendingZeroEmit, key)
			}
		}
	}
	return cloned
}

func InitTelemetry(ctx context.Context, endpoint string, serviceName string) (*Telemetry, error) {
	otlpEndpoint, useInsecureTransport, err := parseOTLPEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse otlp endpoint: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("build telemetry resource: %w", err)
	}

	traceOptions := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(otlpEndpoint),
	}
	if useInsecureTransport {
		traceOptions = append(traceOptions, otlptracehttp.WithInsecure())
	}
	traceExporter, err := otlptracehttp.New(ctx, traceOptions...)
	if err != nil {
		return nil, fmt.Errorf("build otlp trace exporter: %w", err)
	}
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(traceProvider)

	metricOptions := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(otlpEndpoint),
	}
	if useInsecureTransport {
		metricOptions = append(metricOptions, otlpmetrichttp.WithInsecure())
	}
	metricExporter, err := otlpmetrichttp.New(ctx, metricOptions...)
	if err != nil {
		return nil, fmt.Errorf("build otlp metric exporter: %w", err)
	}
	metricProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(metricProvider)

	logOptions := []otlploghttp.Option{
		otlploghttp.WithEndpoint(otlpEndpoint),
	}
	if useInsecureTransport {
		logOptions = append(logOptions, otlploghttp.WithInsecure())
	}
	logExporter, err := otlploghttp.New(ctx, logOptions...)
	if err != nil {
		return nil, fmt.Errorf("build otlp log exporter: %w", err)
	}
	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		sdklog.WithResource(res),
	)
	global.SetLoggerProvider(logProvider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	meter := otel.Meter("ai-advisor-event-aggregator/http")
	inFlightCurrent := newInFlightTracker()
	requestCount, requestDurationMS, errorCount, inFlightRequests, inFlightRegistration, err := initHTTPMetrics(meter, inFlightCurrent)
	if err != nil {
		return nil, err
	}

	return &Telemetry{
		requestCount:      requestCount,
		requestDurationMS: requestDurationMS,
		errorCount:        errorCount,
		inFlightRequests:  inFlightRequests,
		inFlightCurrent:   inFlightCurrent,
		shutdownFns: []func(context.Context) error{
			logProvider.Shutdown,
			metricProvider.Shutdown,
			func(context.Context) error {
				return inFlightRegistration.Unregister()
			},
			traceProvider.Shutdown,
		},
	}, nil
}

func parseOTLPEndpoint(endpoint string) (string, bool, error) {
	parsedURL, err := url.Parse(endpoint)
	if err != nil {
		return "", false, fmt.Errorf("invalid URL %q: %w", endpoint, err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return "", false, fmt.Errorf("unsupported URL scheme %q, expected http or https", parsedURL.Scheme)
	}
	if parsedURL.Host == "" {
		return "", false, errors.New("missing host in URL")
	}
	if parsedURL.User != nil {
		return "", false, errors.New("userinfo in OTLP endpoint is not allowed; configure authentication via supported headers/environment instead")
	}
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		return "", false, fmt.Errorf("path %q is not allowed; use base OTLP endpoint without signal path", parsedURL.Path)
	}
	if parsedURL.RawQuery != "" {
		return "", false, errors.New("query string is not allowed in OTLP endpoint")
	}
	if parsedURL.Fragment != "" {
		return "", false, errors.New("fragment is not allowed in OTLP endpoint")
	}
	return parsedURL.Host, parsedURL.Scheme == "http", nil
}

func initHTTPMetrics(meter metric.Meter, inFlightCurrent *inFlightTracker) (
	metric.Int64Counter,
	metric.Float64Histogram,
	metric.Int64Counter,
	metric.Int64ObservableGauge,
	metric.Registration,
	error,
) {
	requestCount, err := meter.Int64Counter("request_count")
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("create request_count metric: %w", err)
	}
	requestDurationMS, err := meter.Float64Histogram("request_duration_ms", metric.WithUnit("ms"))
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("create request_duration_ms metric: %w", err)
	}
	errorCount, err := meter.Int64Counter("error_count")
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("create error_count metric: %w", err)
	}
	inFlightRequests, err := meter.Int64ObservableGauge("in_flight_requests")
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("create in_flight_requests metric: %w", err)
	}

	inFlightRegistration, err := meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			for key, count := range inFlightCurrent.snapshot() {
				observer.ObserveInt64(
					inFlightRequests,
					count,
					metric.WithAttributes(
						attribute.String("http.method", key.method),
						attribute.String("http.route", key.route),
					),
				)
			}
			return nil
		},
		inFlightRequests,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("register in_flight_requests callback: %w", err)
	}

	return requestCount, requestDurationMS, errorCount, inFlightRequests, inFlightRegistration, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	var errs []error
	for _, shutdown := range t.shutdownFns {
		if err := shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *Telemetry) HTTPMiddleware(next http.Handler) http.Handler {
	if t == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := r.URL.Path
		if r.Pattern != "" {
			route = r.Pattern
		}
		attrs := []attribute.KeyValue{
			attribute.String("http.method", r.Method),
			attribute.String("http.route", route),
		}
		start := time.Now()
		if t.inFlightCurrent != nil {
			t.inFlightCurrent.increment(r.Method, route)
			defer t.inFlightCurrent.decrement(r.Method, route)
		}

		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		defer func() {
			statusAttrs := append(attrs, attribute.Int("http.status_code", recorder.statusCode))
			t.requestCount.Add(r.Context(), 1, metric.WithAttributes(statusAttrs...))
			t.requestDurationMS.Record(
				r.Context(),
				float64(time.Since(start).Milliseconds()),
				metric.WithAttributes(statusAttrs...),
			)
			if recorder.statusCode >= http.StatusBadRequest {
				t.errorCount.Add(r.Context(), 1, metric.WithAttributes(statusAttrs...))
			}
		}()

		next.ServeHTTP(recorder, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (s *statusRecorder) WriteHeader(statusCode int) {
	s.statusCode = statusCode
	s.ResponseWriter.WriteHeader(statusCode)
}
