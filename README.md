# AI Advisor Event Aggregator Service

Go-Backend-Service zur Verarbeitung von News-Artikeln mit Fokus auf:

1. **Preprocessing** von Upstream-Artikeln.
2. **LLM-basierter Extraktion** strukturierter Event-Daten.
3. **Aggregation/Clustering** persistierter Extraction-Ergebnisse zu API-Events.

Der Service ist ein reiner Backend-Dienst ohne Frontend.

## Zweck & Scope

Im Scope:

- Laden von Artikeln über einen Upstream-Service.
- Deterministische Textvorverarbeitung.
- Extraktion strukturierter Events für einzelne oder mehrere Artikel.
- Persistenz von Extraction- und Aggregationsergebnissen in PostgreSQL.
- Auslieferung von Health-, Preprocess-, Extraction- und Event-Endpunkten.

Nicht im Scope:

- Authentifizierung/Autorisierung.
- Eigene TLS-Terminierung (Betrieb hinter Reverse Proxy).

## Architektur & Komponenten

- `main.go`: Konfiguration aus ENV, Dependency Wiring, HTTP-Server, Scheduler-Start.
- `internal/http`: HTTP-Transport, Input-Validierung, Request-ID- und Forwarded-Header-Middleware.
- `internal/upstream`: Upstream-Client zum Abruf von Artikeln.
- `internal/preprocess`: deterministische Textbereinigung.
- `internal/app/extraction`: Extraction-Use-Cases (single/batch + Persistenz).
- `internal/app/events`: Clustering/Aggregation und Event-Abfrage.
- `internal/app/events/scheduler`: periodisches Clustering neuer Extraction-Ergebnisse.
- `internal/app/scheduler`: optionaler Polling-/Batch-Scheduler für automatische Extraktionsläufe.
- `internal/repository/extraction`: PostgreSQL-Repository und Migrationen für Extraction/Event.
- `internal/logging`: strukturierte Logs.
- `internal/observability`: Tracing-Utilities.

## Request Flow

1. Inbound Request läuft durch Middleware (`ForwardedHeaderMiddleware`, `RequestIDMiddleware`) und `otelhttp`-Instrumentation.
2. Handler validieren Methode, Query-Parameter und JSON-Bodies strikt.
3. Handler delegieren in App-Layer (kein DB-Zugriff im Handler).
4. App-Layer nutzt Repository mit parameterisierten SQL-Statements.
5. API liefert JSON-Antworten oder sichere Fehlerantworten.

## API

### `GET /health`

- Liveness-Endpoint.
- Antwort: `200` mit JSON `{ "status": "ok" }`.

### `GET /api/health`

- API-Healthcheck.
- Antwort: `200` mit JSON `{ "status": "ok" }`.

### `GET /api/preprocess`

- Holt Upstream-Artikel und gibt pro Artikel den bereinigten Text zurück.
- Antwort: `200` mit JSON `{"data": [...]}`.
- Fehler:
  - `405` bei falscher HTTP-Methode.
  - `502` wenn Upstream-Artikel nicht geladen werden können.

### `POST /api/extract/run`

- Führt Extraction für genau eine `article_id` aus.
- Request-Body:
  - `article_id` (required, `> 0`)
- Antwort: `200` mit Status (`newly_extracted`, `already_done`, `already_pending`, `failed`) und optionalem Resultat.
- Fehler:
  - `400` bei ungültigem JSON oder ungültiger `article_id`.
  - `404` wenn Artikel nicht gefunden.
  - `502` bei Upstream-Fehler.

### `POST /api/extract/run-batch`

- Führt Extraction für mehrere Artikel aus.
- Request-Body:
  - `article_ids` (required, nicht leer)
  - `concurrency` (optional, `>= 0`)
- Antwort: `200` mit `items`, `total`, `succeeded`, `failed`.
- Fehler:
  - `400` bei ungültigem JSON, leerer `article_ids` oder negativer `concurrency`.
  - `502` bei Upstream-Fehler.
  - `500` wenn Batch-Ausführung intern fehlschlägt.

### `GET /api/extract/result`

- Liest persistiertes Extraction-Ergebnis für `article_id`.
- Query-Parameter:
  - `article_id` (required, positive Ganzzahl)
- Antwort:
  - `200` mit persistierten Feldern inkl. Extraction-Status.
  - `404` falls kein Ergebnis existiert.
- Fehler:
  - `400` bei ungültiger `article_id`.

### `GET /api/events`

- Liefert aggregierte Events aus Persistenz.
- Query-Parameter:
  - `limit` (optional, Default `20`, `> 0`)
  - `since` (optional, RFC3339)
  - `until` (optional, RFC3339)
- Antwort: `200` mit JSON inkl. `limit`, `since`, `until`, `events`.
  - `events` enthält direkt Event-Objekte (kein Wrapper-Objekt pro Eintrag).
  - Beispiel:
    ```json
    {
      "limit": 2,
      "since": "2026-03-30T00:00:00Z",
      "until": "2026-03-30T23:59:59Z",
      "events": [
        {
          "ID": "evt-1",
          "Industries": ["software"]
        }
      ]
    }
    ```
- Fehler:
  - `400` bei ungültigen Query-Parametern.
  - `500` bei internen Ladefehlern.

Kompatibilitätshinweis:
- Das Legacy-Feld `affected_security_count` wird in `GET /api/events` nicht mehr ausgeliefert. API-Consumer müssen dieses Feld entfernen und stattdessen direkt die Event-Objekte unter `events` verarbeiten.

## Validierung & Fehlerverhalten

- Strikte JSON-Validierung (`DisallowUnknownFields`, keine trailing tokens).
- Alle externen Eingaben (Header, Query, Body, Upstream-Payloads) werden als untrusted behandelt und validiert.
- Typische Fehlercodes:
  - `400` invalid input
  - `404` nicht gefunden
  - `405` falsche Methode
  - `500` interner Fehler
  - `502` Upstream-Fehler
- Interne Fehlerdetails werden nur in strukturierten Logs geschrieben, nicht an Clients geleakt.

## Konfiguration (ENV)

### Pflichtvariablen (ohne Default)

- `UPSTREAM_HOST`
- `UPSTREAM_PORT`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `DATABASE_URL` (PostgreSQL DSN)

### Optionale Variablen mit Default

- `PORT` (Default: `8080`)
- `OPENAI_BASE_URL` (Default: OpenAI-Standard `https://api.openai.com/v1`)
- `OPENAI_TIMEOUT_MS` (Default: `15000`)
- `CLUSTER_SCHEDULE_INTERVAL_MINUTES` (Default: `5`)
- `HTTP_SERVER_READ_HEADER_TIMEOUT` (Default: `5s`)
- `HTTP_SERVER_READ_TIMEOUT` (Default: `15s`)
- `HTTP_SERVER_WRITE_TIMEOUT` (Default: `15s`)
- `HTTP_SERVER_IDLE_TIMEOUT` (Default: `60s`)
- `OTEL_EXPORTER_OTLP_ENDPOINT` (Default: `http://localhost:4318`)
- `OTEL_SERVICE_NAME` (Default: `ai-advisor-event-aggregator`)

Scheduler (`internal/app/scheduler`):

- `SCHEDULER_ENABLED` (Default: `false`)
- `SCHEDULER_POLL_INTERVAL_MS` (Default: `30000`)
- `SCHEDULER_PAGE_SIZE` (Default: `50`)
- `SCHEDULER_MAX_PAGES_PER_CYCLE` (Default: `10`)
- `SCHEDULER_DISPATCH_CONCURRENCY` (Default: `2`)
- `SCHEDULER_BATCH_SIZE` (Default: `20`)
- `SCHEDULER_LOG_BATCH_IDS` (Default: `false`)
- `SCHEDULER_LOG_FAILED_ITEMS` (Default: `false`)

No-Op-Kompatibilitätsvariable (wird explizit gelesen, validiert und geloggt, beeinflusst aber kein Runtime-Verhalten):

- `IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES` (Default: `30`, keine funktionale Wirkung)

## Development / Test / Run

```bash
go mod download
go run ./...
```

Tests:

```bash
go test ./...
```

## Build

```bash
go build ./...
```

## CI

- Der Workflow liegt unter `.github/workflows/ci.yml` und läuft bei `pull_request` (alle Branches) sowie bei `push` auf `main`.
- Step-Reihenfolge im Workflow:
  1. checkout
  2. setup runtime
  3. install dependencies
  4. format check (`go fmt ./...` + `git diff --exit-code`)
  5. lint (`go vet ./...`)
  6. static analysis (`staticcheck` via `go run honnef.co/go/tools/cmd/staticcheck@latest ./...`)
  7. security scan (`govulncheck`)
  8. tests (`go test ./...`)
  9. build (`go build ./...`)

## Betriebshinweise / Limitationen

- Betrieb hinter Reverse Proxy; TLS wird extern terminiert.
- HTTP-Server setzt Read/Write/Idle-Timeouts aus ENV.
- Startup ist fail-fast bei ungültiger Konfiguration oder nicht erreichbarer DB.
- DB-Migrationen laufen beim Startup.
- Event-Clustering-Scheduler läuft periodisch mit `CLUSTER_SCHEDULE_INTERVAL_MINUTES`.
- Optionaler Extraction-Scheduler läuft nur mit `SCHEDULER_ENABLED=true`.
