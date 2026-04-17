# AGENTS.md

Mandatory engineering, documentation, and CI rules.  
All rules are binding.

----------------------------------------------------------------
DEFAULTS
----------------------------------------------------------------

- No placeholders
- No skipped steps
- No silent failures
- Documentation must reflect current system state
- environment variables MUST have defaults where possible

----------------------------------------------------------------
LOGGING
----------------------------------------------------------------

- stdout: normal logs
- stderr: errors
- logs MUST be structured
- each log entry MUST contain a human-readable message field
- message MUST be the first field in the log output
- structured fields MUST add machine-readable context without replacing the message
- required fields: message, timestamp, level, request_id, trace_id*

* trace_id if available (OTel enabled)

Message requirements:
- MUST describe what happened, on which entity/resource, and (if applicable) why
- MUST use clear natural language
- MUST be understandable without reading the source code
- MUST NOT be vague, numeric-only, or code-like

Context fields:
- SHOULD include relevant machine-readable context (e.g. entity, operation, outcome, duration_ms, error)
- field names MUST be clear and not abbreviated

Operational logging:
- MUST log important operations and state changes
- MUST log start and completion of relevant operations
- SHOULD include outcome (success/failure) and duration where useful

Errors MUST log:
- failure
- cause (if known)
- sanitized inputs
- system reaction

Panic handling:
- panics MUST be recovered in application-controlled code
- each panic MUST produce one primary structured application error log
- additional logs are allowed if they represent distinct follow-up failures (e.g. abort failure), and MUST add new information
- duplicate logs for the same failure without additional context are forbidden
- panic handling MUST NOT rely on default unstructured server panic logging
- if a panic happens before the response started, the application MUST return a safe error response
- if a panic happens after the response started, the request MUST be aborted and MUST NOT continue as a normal successful response
- panic paths after response start MUST NOT be swallowed silently

- never log secrets
- avoid duplicate or low-value logs

----------------------------------------------------------------
OBSERVABILITY (OTEL)
----------------------------------------------------------------

Use OpenTelemetry only.

Required:
- traces (spans)
- logs
- metrics (request count, latency, error count)
- service.name

Logs:
- application logs MUST be exported via OTLP
- log structure and message content MUST be identical for stdout and OTLP export
- message MUST be the first field in OTLP-exported logs

All telemetry MUST be exported via OTLP over HTTP (default: 4318).

Env:
- OTEL_EXPORTER_OTLP_ENDPOINT (must be used)
- OTEL_SERVICE_NAME

Go:
- official OTel SDK
- batch exporter
- OTLP HTTP exporter
- otelhttp for HTTP
- propagate context (traceparent)

Metrics (minimum):
- request_count (counter)
- request_duration_ms (histogram, milliseconds)
- error_count (counter)
- in_flight_requests (gauge)

Rules:
- no generic span names
- record errors in spans
- always close spans
- metrics MUST use consistent naming and units
- logs, metrics, and traces MUST be correlatable via trace_id

----------------------------------------------------------------
DOCUMENTATION (README.md)
----------------------------------------------------------------

README must fully describe the system.

Include:
- purpose & scope
- architecture & components
- request flow
- configuration (env vars with defaults)
- development / testing / run
- CI
- API endpoints
- validation & error behavior
- operational notes / limitations

Rules:
- no changelog
- always up to date
- no placeholders

----------------------------------------------------------------
TESTING
----------------------------------------------------------------

- unit and integration tests required
- cover normal, edge, and failure cases

----------------------------------------------------------------
CI
----------------------------------------------------------------

Allowed workflow:
- .github/workflows/ci.yml

Triggers:
- pull_request (all branches)
- push (main only)

Fails on any error.

Steps (strict order):
1. checkout
2. setup runtime
3. install dependencies
4. format check
5. lint
6. static analysis
7. security scan (govulncheck or equivalent)
8. tests
9. build

Go minimum:
- go fmt (fail on diff)
- go vet
- go test ./...
- govulncheck

----------------------------------------------------------------
SECURITY
----------------------------------------------------------------

- validate all inputs
- no secrets in code
- avoid sensitive data in responses
- dependencies must have no known vulnerabilities

----------------------------------------------------------------
BACKEND (GO)
----------------------------------------------------------------

- context-aware handlers
- server timeouts (read/write/idle)
- strict JSON validation

HTTP:
- server MUST bind to PORT environment variable (Railway requirement)
- default PORT must be provided if not set

Instrumentation:
- every HTTP handler MUST be instrumented (tracing + metrics)
- every request MUST produce:
  - one root span
  - request_count increment
  - request_duration measurement

Persistence:
- if required:
  - PostgreSQL (Railway)
  - database/sql with pgx (github.com/jackc/pgx/v5/stdlib)

  Env:
  - DATABASE_URL (PostgreSQL connection string, must be used)

  Rules:
  - application MUST connect using DATABASE_URL
  - no hardcoded connection parameters
  - tables: <service>_<entity>
  - repository pattern

- if not required:
  - no database

Error handling:
- full internal logging
- safe external responses
- panics MUST NOT be swallowed silently
- panics after response start MUST abort the request and MUST NOT appear as normal success to clients
- panic handling MUST preserve failure semantics even if no safe error body can be written anymore

----------------------------------------------------------------
FRONTEND
----------------------------------------------------------------

- escape dynamic content
- no dangerouslySetInnerHTML
- no secrets in frontend
- validate inputs (client + server)

----------------------------------------------------------------
ARCHITECTURE
----------------------------------------------------------------

- no new services or infrastructure
- stay within defined architecture

----------------------------------------------------------------
REPOSITORY STRUCTURE
----------------------------------------------------------------

- repository root = service root
- no nested service roots
- no multi-service repositories

Forbidden:
- submodules as services
- multiple go.mod files
- independent service roots in subdirectories

----------------------------------------------------------------
CONSISTENCY
----------------------------------------------------------------

All repositories MUST follow identical CI structure and step ordering.
