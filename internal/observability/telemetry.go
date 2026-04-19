package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	inFlightRequests  metric.Int64UpDownCounter
	shutdownFns       []func(context.Context) error
}

func InitTelemetry(ctx context.Context, endpoint string, serviceName string) (*Telemetry, error) {
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("build telemetry resource: %w", err)
	}

	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("build otlp trace exporter: %w", err)
	}
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(traceProvider)

	metricExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("build otlp metric exporter: %w", err)
	}
	metricProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(metricProvider)

	logExporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(endpoint))
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
	requestCount, err := meter.Int64Counter("request_count")
	if err != nil {
		return nil, fmt.Errorf("create request_count metric: %w", err)
	}
	requestDurationMS, err := meter.Float64Histogram("request_duration_ms", metric.WithUnit("ms"))
	if err != nil {
		return nil, fmt.Errorf("create request_duration_ms metric: %w", err)
	}
	errorCount, err := meter.Int64Counter("error_count")
	if err != nil {
		return nil, fmt.Errorf("create error_count metric: %w", err)
	}
	inFlightRequests, err := meter.Int64UpDownCounter("in_flight_requests")
	if err != nil {
		return nil, fmt.Errorf("create in_flight_requests metric: %w", err)
	}

	return &Telemetry{
		requestCount:      requestCount,
		requestDurationMS: requestDurationMS,
		errorCount:        errorCount,
		inFlightRequests:  inFlightRequests,
		shutdownFns: []func(context.Context) error{
			logProvider.Shutdown,
			metricProvider.Shutdown,
			traceProvider.Shutdown,
		},
	}, nil
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
		t.inFlightRequests.Add(r.Context(), 1, metric.WithAttributes(attrs...))
		defer t.inFlightRequests.Add(r.Context(), -1, metric.WithAttributes(attrs...))

		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)

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
