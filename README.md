# AI Advisor Impact Service

Backend-Service zur Verarbeitung von News-Artikeln mit zwei Kernfunktionen:

1. **Extraktion** strukturierter Event-Daten aus Artikeln.
2. **Aggregation/Clustering** dieser Events für API-Ausgabe.

Der Service ist ein reiner Go-Backend-Dienst ohne Frontend-Runtime.

## Zweck & Scope

- Holt Artikel über einen Upstream-Service.
- Führt deterministische Textvorverarbeitung durch.
- Nutzt ein LLM für Event-Extraktion.
- Persistiert Extraktions- und Aggregationsergebnisse in PostgreSQL.
- Stellt HTTP-Endpunkte für Health, Preprocess, Extraktion und Event-Abfrage bereit.

Nicht im Scope:

- Authentifizierung/Autorisierung (nicht angefordert).
- Eigene TLS-Terminierung (läuft hinter Reverse Proxy).

## Architektur & Komponenten

- `main.go`: Startup, Konfiguration, Dependency Wiring, Scheduler-Start.
- `internal/http`: HTTP-Transport, Request-ID-/Forwarded-Header-Middleware, Input-Validierung.
- `internal/upstream`: Upstream-Client zum Laden von Artikeln.
- `internal/universe`: Universe-HTTP-Client und Upstream-DTOs.
- `internal/universe/store`: thread-safe In-Memory Snapshot Store mit einmaligem Startup-Load.
- `internal/preprocess`: Deterministische Textbereinigung.
- `internal/app/extraction`: Use-Cases für Einzel-/Batch-Extraktion.
- `internal/repository/extraction`: Repository + Runtime-Migrationen für Extraktion/Event.
- `internal/app/events`: Event-Service.
- `internal/app/events/scheduler`: Scheduler für periodisches Clustering neuer Extraction-Ergebnisse.
- `internal/app/scheduler`: Polling-/Batch-Scheduler für automatische Extraktionsläufe.
- `internal/logging`: Strukturierte JSON-Logs.

### Scheduler-Gegenüberstellung

1. `internal/app/scheduler`
   - Zweck: automatische Extraktionsläufe (Polling + Batch-Dispatch).
   - Aktivierung: `SCHEDULER_ENABLED=true`.
   - Relevante Parameter: `SCHEDULER_POLL_INTERVAL_MS`, `SCHEDULER_PAGE_SIZE`, `SCHEDULER_MAX_PAGES_PER_CYCLE`, `SCHEDULER_DISPATCH_CONCURRENCY`, `SCHEDULER_BATCH_SIZE`, `SCHEDULER_LOG_BATCH_IDS`, `SCHEDULER_LOG_FAILED_ITEMS`.
2. `internal/app/events/scheduler`
   - Zweck: periodisches Clustering neuer Extraction-Ergebnisse.
   - Intervall: `CLUSTER_SCHEDULE_INTERVAL_MINUTES`.

## Request-Flow

1. HTTP-Request trifft auf Middleware:
   - `otelhttp` erzeugt serverseitige Inbound-Spans mit route-basierten Namen (`METHOD + ServeMux-Pattern`).
   - `ForwardedHeaderMiddleware` normalisiert Proxy-Metadaten.
   - `RequestIDMiddleware` validiert/vergibt `X-Request-ID`.
2. Handler validiert Methode, Query und Body strikt.
3. Handler delegiert in App-Layer (kein DB-Zugriff im Handler).
4. App-Layer nutzt Repository (parameterisierte SQL-Zugriffe via `database/sql`).
5. Antwort wird als JSON oder definierter Fehlerstatus zurückgegeben.

## Universe-Startup-Flow

1. Beim Prozessstart lädt der Service genau einmal `GET {UNIVERSE_BASE_URL}/api/universe`.
2. Die Universe-Antwort wird strikt validiert (inkl. required Feldern und Duplicate-Code-Prüfung).
3. Validierte Universe-Daten werden in einen immutable Snapshot gemappt und thread-safe im Speicher gehalten.
4. Bei Lade-, Decode- oder Validierungsfehlern bricht der Startup fail-fast ab.

## Impact Domain Model

Das Paket `internal/impact` definiert ein deterministisches, internes Domänenmodell für spätere Impact-Berechnung:

- `SecurityProfile`: normalisiertes Security-Profil (u. a. Identität, GICS-Felder, Geo-/Country-Exposure, Event-Type-Sensitivity, Profile-Confidence).
- `EventSecurityImpact`: ein einzelnes Event→Security-Impact-Ergebnis inkl. Richtung, Scores, Teil-Scores, Rule-Version und maschinenlesbaren Explanation-Codes.
- `SecurityImpactSummary`: aggregierte Sicht pro Security über mehrere Events.

Die Impact-Typen sind bewusst von Upstream-DTOs entkoppelt und enthalten keine Scoring-/Matching-Implementierung. Sie sind die stabile Grundlage für den späteren Rule-Engine-Schritt.

## Impact Rules (V1)

Das Paket `internal/impact/rules` enthält den zentralen, versionierten Regelstand für die spätere Impact-Engine:

- deterministische In-Code-Regeln (kein externer Rule-Engine-Stack),
- Versionierung über `RuleVersion` (`impact_rules_v1`),
- stark typisierte Regelkonfiguration (`MatchingWeights`, `RecencyDecayConfig`, `ScoreConfig`, `SectorFallbackRule`, `DirectionRule`),
- `DefaultRuleSet()` liefert ein vollständig initialisiertes V1-Regelset,
- `Validate()` prüft Konsistenz und sichere Mindestanforderungen.

Es gibt aktuell keine Runtime-Editierung und keinen Dateilader für Regeln; Erweiterungen erfolgen kontrolliert im Code.

## HTTP API

### `GET /health`

- Zweck: einfacher Liveness-Check.
- Antwort: `200` mit Text-Body `ok`.

### `GET /api/health`

- Zweck: JSON-Healthcheck für API-Clients.
- Antwort: `200` mit JSON-Objekt (u. a. Feld `status: "ok"`).

### `GET /api/preprocess`

- Zweck: deterministische Textvorverarbeitung eines Eingabetexts.
- Query-Parameter:
  - `text` (required)
- Antwort: `200` mit JSON inkl. vorverarbeitetem Text.
- Validierung: fehlender/ungültiger Input -> `400`.

### `POST /api/extract/run`

- Zweck: Extraktion für genau einen Artikel starten/fortsetzen.
- Request-Body (JSON):
  - `article_id` (required, `> 0`)
- Antwort: `200` mit JSON-Status (`done|pending|...`) und ggf. Extraktionsresultat.
- Validierung: ungültiger Body oder `article_id <= 0` -> `400`.

### `POST /api/extract/run-batch`

- Zweck: Extraktion für mehrere Artikel in einem Request ausführen.
- Request-Body (JSON):
  - `article_ids` (required, nicht leer, alle `> 0`)
  - `concurrency` (optional, positive Ganzzahl)
- Antwort: `200` mit JSON-Übersicht pro Artikel (`items`, `succeeded`, `failed`, `total`).
- Validierung: ungültiger Body/ungültige IDs -> `400`.

### `GET /api/extract/result`

- Zweck: persistiertes Extraktionsergebnis für eine Artikel-ID lesen.
- Query-Parameter:
  - `article_id` (required, positive Ganzzahl)
- Antwort:
  - `200` mit JSON inkl. `article_id`, `extraction_status`, Modell und extrahierten Feldern
  - `404` wenn kein Ergebnis für die ID existiert
- Validierung: fehlender/ungültiger `article_id` -> `400` (`invalid article_id`).

### `GET /api/events`

- Zweck: aggregierte Events aus Persistenz abrufen.
- Query-Parameter:
  - `limit` (optional, positive Ganzzahl)
  - `since` (optional, RFC3339)
  - `until` (optional, RFC3339)
- Antwort: `200` mit JSON-Liste aggregierter Events und effektiven Filtern.
- Validierung: ungültige Query-Parameter -> `400`.

## Validierung & Fehlerverhalten

- Ungültige Methoden -> `405`.
- Ungültige Query-/Body-Werte -> `400`.
- Nicht gefundene Ressource (z. B. Artikel-ID) -> `404`.
- Upstream-Fehler -> `502`.
- Nicht konfigurierte Abhängigkeiten -> `500` mit sicherer, generischer Fehlermeldung.
- Interne Fehlerdetails werden nicht an Clients geleakt; Details nur in strukturierten Logs.

## Konfiguration (ENV)

### Pflichtvariablen

- `PORT`
- `UPSTREAM_HOST`
- `UPSTREAM_PORT`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `UNIVERSE_BASE_URL`
- `EXTRACT_DB_DSN`
  - Unterstützte Formate (lib/pq-kompatibel):
    - URL-Format, z. B. `postgres://user:pass@localhost:5432/appdb?sslmode=disable`
    - Key/Value-Format, z. B. `host=localhost port=5432 user=user password=pass dbname=appdb sslmode=disable`

### Optionale Variablen mit Defaults

- `OPENAI_BASE_URL` (Default: OpenAI SDK-Standard)
- `OPENAI_TIMEOUT_MS` (Default: `15000`)
- `UNIVERSE_TIMEOUT_MS` (Default: `10000`)
- `CLUSTER_SCHEDULE_INTERVAL_MINUTES` (Default: `5`)
- `HTTP_SERVER_READ_HEADER_TIMEOUT` (Default: `5s`)
- `HTTP_SERVER_READ_TIMEOUT` (Default: `15s`)
- `HTTP_SERVER_WRITE_TIMEOUT` (Default: `15s`)
- `HTTP_SERVER_IDLE_TIMEOUT` (Default: `60s`)
- `IMPACT_MIN_SCORE` (Default: `10`)
- `IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES` (Default: `30`, aktuell reserviert)

#### Extraktions-Scheduler (`internal/app/scheduler`)

Die Scheduler-Konfiguration wird über `internal/app/scheduler/config.go::ParseConfigFromEnv` geladen; bei ungültigen Werten bricht der Startup mit Fehler ab.

- `SCHEDULER_ENABLED`
  - Default: `false`
  - Typ/Wertebereich: bool (`true|false`, akzeptiert Go-`strconv.ParseBool`-Formate)
  - Wirkung: Aktiviert/deaktiviert den in-process Scheduler für automatisches Polling und Dispatching von Extraktionsläufen.
- `SCHEDULER_POLL_INTERVAL_MS`
  - Default: `30000` (30s)
  - Typ/Wertebereich: Integer in Millisekunden, `> 0`
  - Wirkung: Steuert das Polling-Intervall zwischen zwei Scheduler-Zyklen.
- `SCHEDULER_PAGE_SIZE`
  - Default: `50`
  - Typ/Wertebereich: Integer, `> 0`
  - Wirkung: Begrenzt, wie viele Kandidaten pro geladener Seite je Polling-Zyklus verarbeitet werden.
- `SCHEDULER_MAX_PAGES_PER_CYCLE`
  - Default: `10`
  - Typ/Wertebereich: Integer, `> 0`
  - Wirkung: Begrenzt die maximale Anzahl geladener Seiten pro Scheduler-Zyklus.
- `SCHEDULER_DISPATCH_CONCURRENCY`
  - Default: `2`
  - Typ/Wertebereich: Integer, `> 0`
  - Wirkung: Legt die parallele Verarbeitung beim Dispatch von Extraktionsjobs fest.
- `SCHEDULER_BATCH_SIZE`
  - Default: `20`
  - Typ/Wertebereich: Integer, `> 0`
  - Wirkung: Definiert die Größe einzelner Batch-Requests pro Dispatch.
- `SCHEDULER_LOG_BATCH_IDS`
  - Default: `false`
  - Typ/Wertebereich: bool (`true|false`, akzeptiert Go-`strconv.ParseBool`-Formate)
  - Wirkung: Aktiviert zusätzliche Logs mit Batch-IDs für bessere Nachvollziehbarkeit.
- `SCHEDULER_LOG_FAILED_ITEMS`
  - Default: `false`
  - Typ/Wertebereich: bool (`true|false`, akzeptiert Go-`strconv.ParseBool`-Formate)
  - Wirkung: Aktiviert zusätzliche Logs fehlgeschlagener Batch-Items.
- `IMPACT_RECALC_SCHEDULE_INTERVAL_MINUTES`
  - Default: `30`
  - Typ/Wertebereich: Integer in Minuten, `> 0`
  - Wirkung: Reservierte Kompatibilitätsvariable (derzeit kein aktiver Recalculator-Job).

Ohne `SCHEDULER_ENABLED=true` findet keine automatische Extraktions-Polling/Batch-Verarbeitung statt; Extraktion erfolgt dann nur über API-Endpoints.
`UNIVERSE_BASE_URL` und `UNIVERSE_TIMEOUT_MS` werden beim Startup für den einmaligen Universe-Load verwendet. Fehlende/ungültige Werte oder eine ungültige Universe-Antwort führen zum Startup-Abbruch.

## Observability (OpenTelemetry)

Der Service implementiert OpenTelemetry Tracing konkret wie folgt:

- Startup initialisiert einen OTLP/HTTP Trace-Exporter und einen globalen `TracerProvider` mit Batch-Processor.
- `service.name` wird als Resource-Attribut gesetzt.
- Globaler Propagator ist `tracecontext + baggage`.
- HTTP-Inbound wird über `otelhttp` instrumentiert (pro Request ein Server-Span mit route-basiertem Namen).
- Zusätzliche Spans sind in den zentralen Use-Cases implementiert:
  - Extraktion (`extraction.run`, `extraction.batch_run`)
  - Event-/Impact-Queries
  - Upstream-/Universe-Fetch
  - zentrale Repository-Methoden (Extraction + Impact)
- Fehler werden in Spans via `RecordError` und Error-Status markiert.
- Beim Shutdown wird der TracerProvider mit Timeout beendet, damit Batch-Spans geflusht werden.

Wichtige Umgebungsvariablen:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (Default: `http://localhost:4318`)
- `OTEL_SERVICE_NAME` (Default: `ai-advisor-impact-service`)

## Logging

- Strukturierte JSON-Logs mit Feldern wie `ts`, `level`, `event`, `msg`, `request_id`, `component`.
- Wenn Trace-Kontext vorhanden ist, enthalten Logs zusätzlich `trace_id` und `span_id`.
- `stdout`: normale Logs.
- `stderr`: Fehlerlogs.
- Keine Secrets in Logs.
- **Verpflichtender Error-Log-Contract** für alle `level=error` Einträge:
  - `failure`: kurze, stabile Fehlerklassifikation (z. B. `events_query_failed`).
  - `cause`: technische Fehlerursache (aus internem Fehlerobjekt).
  - `sanitized_input`: sanitisiertes Input-Snapshot (nie Secrets, Tokens oder vollständige DSNs).
  - `reaction`: Systemreaktion (z. B. `returned http 500`, `process exit with status 1`).
- High-Impact-Pfade (HTTP-Handler, Startup/Shutdown, Upstream/Universe/Repository-Close-Fehler) schreiben diese Felder konsistent.

## Entwicklung, Test, Run

```bash
go mod download
go test ./...
go test -tags=integration ./...
go run ./...
```

## Build

```bash
go build ./...
```

## CI

Verwendeter Workflow:

- `.github/workflows/ci.yml`

Reihenfolge der CI-Schritte:

1. Checkout
2. Runtime Setup
3. Dependencies installieren
4. Format-Check
5. Lint
6. Statische Analyse
7. Security-Scan
8. Tests
9. Build

## Betriebshinweise / Limitationen

- Service läuft hinter Reverse Proxy; TLS wird extern terminiert.
- Forwarded Header werden normalisiert, aber Requestdaten bleiben untrusted und werden pro Endpoint validiert.
- Startup ist Fail-Fast bei ungültiger Konfiguration.
- Repository-Migrationen für Extraktion/Events laufen beim Startup.
- Universe-Daten werden nur einmal beim Startup geladen und ausschließlich in-memory gehalten (kein Refresh, kein Scheduler, keine Persistenz, kein Reload-Endpunkt).
### Scheduler-Betriebsmodi (main.go)

- **Modus A: `SCHEDULER_ENABLED=false`**
  - `internal/app/events/scheduler` läuft und clustert periodisch gemäß `CLUSTER_SCHEDULE_INTERVAL_MINUTES`.
- **Modus B: `SCHEDULER_ENABLED=true`**
  - `internal/app/events/scheduler` läuft weiterhin für Clustering gemäß `CLUSTER_SCHEDULE_INTERVAL_MINUTES`.
  - `internal/app/scheduler` übernimmt zusätzliche automatische Extraktions-Polling/Batch-Läufe.
