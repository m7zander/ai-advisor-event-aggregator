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

## Rollout-Hinweise für bestehende Alt-DBs

Für Alt-DBs gelten folgende klare Rollout-Regeln:

1. Runtime deployen, damit `Repository.Migrate` die aktiven Tabellen (`event_aggregator_*`) sicherstellt und bestehende `impact_service_*`-Tabellen/Indexnamen idempotent auf die aktiven Namen migriert.
2. Verifizieren, dass produktive Reads/Writes nur noch auf aktiven Tabellen laufen und keine Legacy-Impact-Tabellen mehr verwenden (insbesondere `event_security_impacts` bzw. `impact_service_event_security_impacts`).
3. Optionalen Decommission getrennt planen: `legacy_archive/000009_decommission_legacy_impact_tables.sql` **nicht** im Startup ausführen, sondern als separaten, expliziten Ops-Schritt.
4. Vor jedem physischen Drop Snapshot/Backup erstellen; ein Code-Rollback allein stellt gedroppte Tabellen nicht wieder her.
5. Für bereits bereinigte DBs ist kein zusätzlicher SQL-Runtime-Schritt nötig, da die Anwendung ausschließlich den Code-Migrationspfad nutzt.

## Policy: keine doppelte Ownership

Schemaänderungen an den kanonisch aktiven Tabellen dürfen nur über **einen** Pfad eingeführt werden: Runtime-Migrationen im Code.

Damit werden doppelte DDL-Ausführungen und inkonsistente Deployments vermieden.
