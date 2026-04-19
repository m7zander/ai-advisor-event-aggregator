<!-- This file documents canonical runtime schema ownership and archived SQL history. -->

# Migration Ownership

## Aktiver produktiver Startup-Migrationspfad

Der produktive Startup-Migrationspfad läuft **ausschließlich** über Runtime-Migrationen im Code:

- `internal/repository/extraction.Repository.Migrate`

Die Anwendung lädt beim Startup **keine SQL-Dateien aus `migrations/`**. Der Runtime-Mechanismus besteht ausschließlich aus den in `Repository.Migrate` hinterlegten DDL-Statements.

Dieser Pfad deckt nur den verbleibenden Service-Scope ab und erzeugt/verwaltet ausschließlich diese **aktiven** Tabellen:

- `event_aggregator_article_extractions`
- `event_aggregator_aggregated_events`
- `event_aggregator_clustering_state`

Es gibt keinen zweiten produktiven SQL-Dateipfad unter `migrations/`, der beim App-Startup ausgeführt wird.

## Rein historische Legacy-SQL-Dateien (nicht runtime-relevant)

Alle historischen SQL-Dateien wurden nach `migrations/legacy_archive/` verschoben und sind nur noch Dokumentations-/Audit-Historie:

- `legacy_archive/000001_create_article_extractions.sql`
- `legacy_archive/000002_drop_extracted_duplicate_candidate.sql`
- `legacy_archive/000003_create_aggregated_events.sql`
- `legacy_archive/000004_add_extracted_industries.sql`
- `legacy_archive/000005_add_aggregated_event_industries.sql`
- `legacy_archive/000006_create_event_security_impacts.sql`
- `legacy_archive/000007_drop_event_security_impacts.sql`
- `legacy_archive/000008_rename_legacy_tables_with_service_prefix.sql`
- `legacy_archive/000009_decommission_legacy_impact_tables.sql`

### Verbindliche Abgrenzung

`migrations/legacy_archive/*.sql` ist ein **reines Audit-/Compliance-Archiv**.

- Diese Dateien sind **nicht Teil des Runtime-Migrationspfads**.
- Diese Dateien werden beim Startup **niemals** geladen oder ausgeführt.
- Die **einzige** aktive Migrationseintrittsstelle im Service ist:
  - `internal/repository/extraction/repository.go` → `(*Repository).Migrate`

## Sunset-Entscheidung für Legacy-Rename-Migrationen

**Verbindliche Entscheidung:** Die automatische Alt-DB-Rename-Migration für unpräfixierte Tabellen (`article_extractions`, `aggregated_events`, `clustering_state`) und `impact_service_*`-Namen wird im Runtime-Code **nicht mehr unterstützt**.

**Cutover-Version:** `runtime-schema-cutover-2026-04-19`.

Ab dieser Cutover-Version gilt produktiv ausschließlich der kanonische Runtime-Schema-Pfad mit `event_aggregator_*`-Tabellen/Indizes. Der Startup-Pfad führt keine `ALTER TABLE ... RENAME ...` oder `ALTER INDEX ... RENAME ...` Legacy-Schritte mehr aus.

### Operative Konsequenz für Alt-DBs

1. Datenbanken mit Legacy-Tabellennamen müssen **vor** Deployment auf `runtime-schema-cutover-2026-04-19` durch einen expliziten, separat geplanten Ops-Migrationsschritt auf `event_aggregator_*` umgestellt werden.
2. Die Anwendung erstellt ab Cutover nur noch fehlende kanonische Tabellen/Indizes und führt kanonische Legacy-Fixups (Spaltenergänzungen innerhalb `event_aggregator_*`) idempotent aus.
3. Historische SQL-Dateien in `migrations/legacy_archive/` bleiben Audit-Historie und sind weiterhin kein Bestandteil des Runtime-Startup-Pfads.

## Policy: keine doppelte Ownership

Schemaänderungen an den kanonisch aktiven Tabellen dürfen nur über **einen** Pfad eingeführt werden: Runtime-Migrationen im Code.

Damit werden doppelte DDL-Ausführungen und inkonsistente Deployments vermieden.
