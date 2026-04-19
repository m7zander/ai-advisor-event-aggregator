// Command main configures and starts the HTTP server with upstream, extraction, and persistence dependencies.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	appevents "ai-advisor-event-aggregator/internal/app/events"
	appextraction "ai-advisor-event-aggregator/internal/app/extraction"
	appscheduler "ai-advisor-event-aggregator/internal/app/scheduler"
	httpapi "ai-advisor-event-aggregator/internal/http"
	"ai-advisor-event-aggregator/internal/logging"
	"ai-advisor-event-aggregator/internal/upstream"

	repopkg "ai-advisor-event-aggregator/internal/repository/extraction"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	defaultPort                  = 8080
	defaultHTTPReadHeaderTimeout = 5 * time.Second
	defaultHTTPReadTimeout       = 15 * time.Second
	defaultHTTPWriteTimeout      = 15 * time.Second
	defaultHTTPIdleTimeout       = 60 * time.Second
)

type httpServerTimeoutConfig struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// main reads environment configuration, wires application dependencies, and runs the HTTP server.
// It requires upstream, OpenAI, and extraction persistence configuration values from environment variables.
// It terminates the process on invalid configuration or startup failures.
//
// Trust boundary note: this service runs behind Railway's reverse proxy. External TLS is terminated at
// the proxy and this process serves plain HTTP internally. The service intentionally does not configure
// TLS listeners or local HTTP→HTTPS redirects; scheme/client metadata is derived from trusted
// X-Forwarded-* headers at the HTTP transport layer.
func main() {
	logger := logging.New()

	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	portRaw := os.Getenv("PORT")
	if portRaw == "" {
		portRaw = strconv.Itoa(defaultPort)
	}
	host := os.Getenv("UPSTREAM_HOST")
	upstreamPort := os.Getenv("UPSTREAM_PORT")
	openAIAPIKey := os.Getenv("OPENAI_API_KEY")
	openAIModel := os.Getenv("OPENAI_MODEL")
	openAIBaseURL := os.Getenv("OPENAI_BASE_URL")
	openAITimeoutMSRaw := os.Getenv("OPENAI_TIMEOUT_MS")
	dbDSN := os.Getenv("DATABASE_URL")
	clusterScheduleMinutesRaw := os.Getenv("CLUSTER_SCHEDULE_INTERVAL_MINUTES")
	otelEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	otelServiceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))

	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 {
		fatalf(logger, "PORT must be a valid positive integer, got %q", portRaw)
	}
	if host == "" {
		fatalf(logger, "UPSTREAM_HOST is required")
	}
	if upstreamPort == "" {
		fatalf(logger, "UPSTREAM_PORT is required")
	}
	if openAIAPIKey == "" {
		fatalf(logger, "OPENAI_API_KEY is required")
	}
	if openAIModel == "" {
		fatalf(logger, "OPENAI_MODEL is required")
	}
	if otelEndpoint == "" {
		otelEndpoint = "http://localhost:4318"
	}
	if otelServiceName == "" {
		otelServiceName = "ai-advisor-event-aggregator"
	}
	if dbDSN == "" {
		fatalf(logger, "DATABASE_URL is required")
	}
	if err := validatePostgresDSN(dbDSN); err != nil {
		fatalf(logger, "DATABASE_URL must be a valid PostgreSQL DSN: %v", err)
	}
	schedulerCfg, err := appscheduler.ParseConfigFromEnv()
	if err != nil {
		fatalf(logger, "invalid scheduler config: %v", err)
	}
	serverTimeouts, err := loadHTTPServerTimeoutConfigFromEnv()
	if err != nil {
		fatalf(logger, "invalid http server timeout config: %v", err)
	}
	telemetryShutdown, err := initTelemetry(rootCtx, otelEndpoint, otelServiceName)
	if err != nil {
		fatalf(logger, "failed to initialize telemetry: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if shutdownErr := telemetryShutdown(shutdownCtx); shutdownErr != nil {
			logger.ErrorWithContract(rootCtx, "app.telemetry.shutdown_failed", "main", "telemetry shutdown failed", shutdownErr, logging.ErrorContract{
				Failure:        "telemetry_shutdown_failed",
				Cause:          shutdownErr.Error(),
				SanitizedInput: "{}",
				Reaction:       "continuing process shutdown",
			})
		}
	}()

	timeoutMS := 15000
	if openAITimeoutMSRaw != "" {
		parsed, parseErr := strconv.Atoi(openAITimeoutMSRaw)
		if parseErr != nil || parsed <= 0 {
			fatalf(logger, "OPENAI_TIMEOUT_MS must be a valid positive integer, got %q", openAITimeoutMSRaw)
		}
		timeoutMS = parsed
	}
	clusterScheduleMinutes := 5
	if clusterScheduleMinutesRaw != "" {
		parsed, parseErr := strconv.Atoi(clusterScheduleMinutesRaw)
		if parseErr != nil || parsed <= 0 {
			fatalf(logger, "CLUSTER_SCHEDULE_INTERVAL_MINUTES must be a valid positive integer, got %q", clusterScheduleMinutesRaw)
		}
		clusterScheduleMinutes = parsed
	}

	db, err := sql.Open("pgx", dbDSN)
	if err != nil {
		fatalf(logger, "failed to open extraction DB: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			logger.ErrorWithContract(rootCtx, "app.db.close_failed", "main", "failed to close extraction DB", cerr, logging.ErrorContract{
				Failure:        "extract_db_close_failed",
				Cause:          cerr.Error(),
				SanitizedInput: `{"resource":"extract_db"}`,
				Reaction:       "continuing process shutdown",
			},
				logging.Field{Key: "resource", Value: "extract_db"})
		}
	}()
	if err := db.PingContext(rootCtx); err != nil {
		fatalf(logger, "failed to ping extraction DB: %v", err)
	}

	repo, err := repopkg.NewRepository(db)
	if err != nil {
		fatalf(logger, "failed to create extraction repository: %v", err)
	}
	if err := repo.Migrate(context.Background()); err != nil {
		fatalf(logger, "failed to run extraction migrations: %v", err)
	}
	extractor, err := appextraction.NewLLMExtractor(appextraction.LLMConfig{
		APIKey:  openAIAPIKey,
		Model:   openAIModel,
		BaseURL: openAIBaseURL,
		Timeout: time.Duration(timeoutMS) * time.Millisecond,
	})
	if err != nil {
		fatalf(logger, "failed to build extraction client: %v", err)
	}
	clusterService, err := appevents.NewService(repo)
	if err != nil {
		fatalf(logger, "failed to build clustering service: %v", err)
	}

	mux := http.NewServeMux()
	client := upstream.NewClient(host, upstreamPort)
	h := httpapi.NewHandlerWithExtractionAndClustering(client, extractor, repo, openAIModel, clusterService)
	h.Register(mux)
	eventScheduler := appevents.NewScheduler(clusterService, logger, time.Duration(clusterScheduleMinutes)*time.Minute)
	if eventScheduler == nil {
		fatalf(logger, "failed to build event scheduler")
	}
	go eventScheduler.Run(rootCtx)
	logger.Info(rootCtx, "app.event_scheduler.enabled", "main", "event scheduler enabled", logging.Field{Key: "interval_minutes", Value: clusterScheduleMinutes})

	if schedulerCfg.Enabled {
		scheduler, err := appscheduler.New(
			appscheduler.NewUpstreamClientAdapter(client),
			appscheduler.NewAppBatchRunner(extractor, repo, openAIModel),
			logger,
			schedulerCfg,
		)
		if err != nil {
			fatalf(logger, "failed to build scheduler: %v", err)
		}
		go func() {
			if runErr := scheduler.Run(rootCtx); runErr != nil {
				logger.ErrorWithContract(rootCtx, "app.scheduler.stopped_error", "main", "scheduler stopped with error", runErr, logging.ErrorContract{
					Failure:        "scheduler_stopped_with_error",
					Cause:          runErr.Error(),
					SanitizedInput: "{}",
					Reaction:       "scheduler worker exited",
				})
			}
		}()
		logger.Info(rootCtx, "app.scheduler.enabled", "main", "scheduler enabled",
			logging.Field{Key: "poll_interval", Value: schedulerCfg.PollInterval.String()},
			logging.Field{Key: "page_size", Value: schedulerCfg.PageSize},
			logging.Field{Key: "max_pages", Value: schedulerCfg.MaxPagesPerCycle},
			logging.Field{Key: "dispatch_concurrency", Value: schedulerCfg.DispatchConcurrency},
			logging.Field{Key: "batch_size", Value: schedulerCfg.BatchSize},
		)
	} else {
		logger.Info(rootCtx, "app.scheduler.disabled", "main", "scheduler disabled")
	}

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr: addr,
		// Middleware order: first normalize trusted proxy metadata, then attach/validate request IDs.
		// Handlers must treat all body/query/header values as untrusted input even behind the proxy.
		Handler: httpapi.RequestIDMiddleware(httpapi.ForwardedHeaderMiddleware(
			otelhttp.NewHandler(
				mux,
				"http.server",
				otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
					_, pattern := mux.Handler(r)
					pattern = strings.TrimSpace(pattern)
					if pattern == "" {
						pattern = "/unmatched"
					}
					return fmt.Sprintf("%s %s", r.Method, pattern)
				}),
			),
		)),
		ReadHeaderTimeout: serverTimeouts.ReadHeaderTimeout,
		ReadTimeout:       serverTimeouts.ReadTimeout,
		WriteTimeout:      serverTimeouts.WriteTimeout,
		IdleTimeout:       serverTimeouts.IdleTimeout,
	}
	logger.Info(rootCtx, "app.server.timeouts_configured", "main", "http server timeouts configured",
		logging.Field{Key: "read_header_timeout", Value: serverTimeouts.ReadHeaderTimeout.String()},
		logging.Field{Key: "read_timeout", Value: serverTimeouts.ReadTimeout.String()},
		logging.Field{Key: "write_timeout", Value: serverTimeouts.WriteTimeout.String()},
		logging.Field{Key: "idle_timeout", Value: serverTimeouts.IdleTimeout.String()},
	)
	logger.Info(rootCtx, "app.server.listening", "main", "listening", logging.Field{Key: "addr", Value: addr})
	go func() {
		<-rootCtx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.ErrorWithContract(rootCtx, "app.server.shutdown_error", "main", "http shutdown error", shutdownErr, logging.ErrorContract{
				Failure:        "http_server_shutdown_error",
				Cause:          shutdownErr.Error(),
				SanitizedInput: "{}",
				Reaction:       "process continues shutdown",
			})
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatalf(logger, "server failed: %v", err)
	}
	logger.Info(rootCtx, "app.server.stopped", "main", "server stopped")
}

func initTelemetry(ctx context.Context, endpoint string, serviceName string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("build otlp exporter: %w", err)
	}
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("build telemetry resource: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tracerProvider.Shutdown, nil
}

// fatalf logs a startup/runtime fatal error with an explicit error level and exits with status code 1.
// The logger parameter is the shared application logger used for all process output.
// The format and args parameters follow fmt.Printf formatting semantics for the error message.
// It always terminates the process via os.Exit(1) after writing the log line.
func fatalf(logger *logging.Logger, format string, args ...any) {
	err := fmt.Errorf("%s", fmt.Sprintf(format, args...))
	logger.ErrorWithContract(context.Background(), "app.startup.fatal", "main", "fatal startup error", err, logging.ErrorContract{
		Failure:        "startup_or_runtime_fatal",
		Cause:          err.Error(),
		SanitizedInput: "redacted",
		Reaction:       "process exit with status 1",
	})
	os.Exit(1)
}

// validatePostgresDSN verifies that dsn follows PostgreSQL DSN formats supported by pgx stdlib.
// The dsn parameter is the DATABASE_URL value from environment variables.
// It returns nil for valid PostgreSQL connection strings and an error for invalid or unsupported DSNs.
// It fails for empty/whitespace-only input to enforce fail-fast startup behavior.
func validatePostgresDSN(dsn string) error {
	trimmed := strings.TrimSpace(dsn)
	if trimmed == "" {
		return fmt.Errorf("dsn is empty")
	}

	if strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://") {
		parsedURL, err := url.Parse(trimmed)
		if err != nil {
			return fmt.Errorf("invalid URL DSN: %w", err)
		}
		if parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql" {
			return fmt.Errorf("invalid URL DSN scheme %q", parsedURL.Scheme)
		}
		return nil
	}

	parts, err := splitPostgresKVDSN(trimmed)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return fmt.Errorf("invalid key/value DSN: no parameters found")
	}
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid key/value DSN segment %q", part)
		}
		if strings.TrimSpace(kv[0]) == "" {
			return fmt.Errorf("invalid key/value DSN segment %q: key is empty", part)
		}
	}
	return nil
}

func splitPostgresKVDSN(dsn string) ([]string, error) {
	var (
		parts    []string
		current  strings.Builder
		inQuote  bool
		escaping bool
	)
	for _, r := range dsn {
		switch {
		case escaping:
			current.WriteRune(r)
			escaping = false
		case r == '\\':
			current.WriteRune(r)
			escaping = true
		case r == '\'':
			current.WriteRune(r)
			inQuote = !inQuote
		case !inQuote && (r == ' ' || r == '\t' || r == '\n'):
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if escaping {
		return nil, fmt.Errorf("invalid key/value DSN: unfinished escape sequence")
	}
	if inQuote {
		return nil, fmt.Errorf("invalid key/value DSN: unmatched quote")
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts, nil
}

func loadHTTPServerTimeoutConfigFromEnv() (httpServerTimeoutConfig, error) {
	readHeaderTimeout, err := parsePositiveDurationFromEnv("HTTP_SERVER_READ_HEADER_TIMEOUT", defaultHTTPReadHeaderTimeout)
	if err != nil {
		return httpServerTimeoutConfig{}, err
	}
	readTimeout, err := parsePositiveDurationFromEnv("HTTP_SERVER_READ_TIMEOUT", defaultHTTPReadTimeout)
	if err != nil {
		return httpServerTimeoutConfig{}, err
	}
	writeTimeout, err := parsePositiveDurationFromEnv("HTTP_SERVER_WRITE_TIMEOUT", defaultHTTPWriteTimeout)
	if err != nil {
		return httpServerTimeoutConfig{}, err
	}
	idleTimeout, err := parsePositiveDurationFromEnv("HTTP_SERVER_IDLE_TIMEOUT", defaultHTTPIdleTimeout)
	if err != nil {
		return httpServerTimeoutConfig{}, err
	}
	return httpServerTimeoutConfig{
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}, nil
}

func parsePositiveDurationFromEnv(key string, defaultValue time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration (examples: 500ms, 5s, 1m), got %q", key, raw)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero, got %q", key, raw)
	}
	return parsed, nil
}
