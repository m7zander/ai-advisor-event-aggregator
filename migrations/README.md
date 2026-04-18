<!-- This file documents canonical runtime schema ownership and archived SQL history. -->

# Migration Ownership

## Produktiver Startup-Migrationspfad (kanonischer Service-Scope)

Der produktive Startup-Migrationspfad läuft **ausschließlich** über Runtime-Migrationen im Code:

- `internal/repository/extraction.Repository.Migrate`

Dieser Pfad deckt nur den verbleibenden Service-Scope ab und erzeugt/verwaltet ausschließlich diese kanonisch aktiven Tabellen:

- `impact_service_article_extractions`
- `impact_service_aggregated_events`
- `impact_service_clustering_state`

Es gibt keinen zweiten produktiven SQL-Dateipfad unter `migrations/`, der beim App-Startup ausgeführt wird.

## Archivierte Legacy-SQL-Dateien (nicht runtime-relevant)

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

Diese Dateien sind **nicht** Teil des produktiven Runtime-Migrationsmechanismus.

## Rollout-Hinweise für bestehende Alt-DBs

Für Alt-DBs gelten folgende klare Rollout-Regeln:

1. Runtime deployen, damit `Repository.Migrate` die kanonischen Service-Tabellen (`impact_service_*`) sicherstellt.
2. Verifizieren, dass keine produktiven Reads/Writes mehr gegen Legacy-Impact-Tabellen laufen (insbesondere `event_security_impacts` bzw. `impact_service_event_security_impacts`).
3. Falls Legacy-Impact-Tabellen physisch entfernt werden sollen, `legacy_archive/000009_decommission_legacy_impact_tables.sql` als **separaten, geplanten Decommission-Schritt** ausführen (nicht als Teil des App-Startups).
4. Vor Drops Snapshot/Backup sicherstellen; ein Code-Rollback allein stellt gedroppte Tabellen nicht wieder her.

## Policy: keine doppelte Ownership

Schemaänderungen an den kanonisch aktiven Tabellen dürfen nur über **einen** Pfad eingeführt werden: Runtime-Migrationen im Code.

Damit werden doppelte DDL-Ausführungen und inkonsistente Deployments vermieden.
