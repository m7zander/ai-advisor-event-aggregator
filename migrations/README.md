# Migration Ownership

## Runtime aktiv

Der produktive Startup-Migrationspfad läuft ausschließlich über Runtime-Migrationen im Code:

- `internal/repository/extraction/repository.go` → `(*Repository).Migrate`

Beim Startup werden keine SQL-Dateien aus `migrations/` geladen oder ausgeführt.

Aktiv verwaltet werden ausschließlich die kanonischen Tabellen:

- `event_aggregator_article_extractions`
- `event_aggregator_aggregated_events`
- `event_aggregator_clustering_state`

## Historische Referenz (Compliance)

Historische Legacy-SQL-Dateien sind kein Runtime-Bestandteil mehr und wurden im Cutover `runtime-schema-cutover-2026-04-19` in ein externes Compliance-Archiv ausgelagert:

- `compliance://event-aggregator/sql-legacy-archive/runtime-schema-cutover-2026-04-19/`

`migrations/legacy_archive/` enthält nur die Referenzdokumentation auf dieses Archiv und ist nicht Teil des Runtime-Migrationspfads.

## Verbindliche Abgrenzung

- Runtime aktiv: nur `(*Repository).Migrate`.
- Historische Referenz: nur Compliance-Nachweis, keine Ausführung im Startup.
- Legacy-Rename-Migrationen (`ALTER ... RENAME ...`) werden im Runtime-Code nicht mehr unterstützt.
