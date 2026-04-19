<!-- This file documents canonical runtime schema ownership and legacy SQL compliance handling. -->

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

## Finale Policy für historische Legacy-SQL-Dateien

**Verbindliche Entscheidung (final):** `legacy_archive/*.sql` bleibt **nicht** dauerhaft im Service-Repository. Die Dateien werden in ein separates Compliance-Archiv ausgelagert.

Auslagerungsziel:

- `compliance://event-aggregator/sql-legacy-archive/runtime-schema-cutover-2026-04-19/`

Im Service-Repository verbleibt unter `migrations/legacy_archive/` nur noch eine knappe Referenz-Dokumentation mit Dateiliste und Zielreferenz.

### Verbindliche Abgrenzung

`migrations/legacy_archive/` ist nach dem Cutover **kein SQL-Archiv im Repo mehr**, sondern nur ein Verweis auf das externe Compliance-Archiv.

- Diese Referenzdatei ist **nicht Teil des Runtime-Migrationspfads**.
- Beim Startup werden aus `migrations/legacy_archive/` **keine SQL-Dateien** geladen oder ausgeführt.
- Die **einzige** aktive Migrationseintrittsstelle im Service ist:
  - `internal/repository/extraction/repository.go` → `(*Repository).Migrate`

## Sunset-Entscheidung für Legacy-Rename-Migrationen

**Verbindliche Entscheidung:** Die automatische Alt-DB-Rename-Migration für unpräfixierte Tabellen (`article_extractions`, `aggregated_events`, `clustering_state`) und `impact_service_*`-Namen wird im Runtime-Code **nicht mehr unterstützt**.

**Cutover-Version:** `runtime-schema-cutover-2026-04-19`.

Ab dieser Cutover-Version gilt produktiv ausschließlich der kanonische Runtime-Schema-Pfad mit `event_aggregator_*`-Tabellen/Indizes. Der Startup-Pfad führt keine `ALTER TABLE ... RENAME ...` oder `ALTER INDEX ... RENAME ...` Legacy-Schritte mehr aus.

### Operative Konsequenz für Alt-DBs

1. Datenbanken mit Legacy-Tabellennamen müssen **vor** Deployment auf `runtime-schema-cutover-2026-04-19` durch einen expliziten, separat geplanten Ops-Migrationsschritt auf `event_aggregator_*` umgestellt werden.
2. Die Anwendung erstellt ab Cutover nur noch fehlende kanonische Tabellen/Indizes und führt kanonische Legacy-Fixups (Spaltenergänzungen innerhalb `event_aggregator_*`) idempotent aus.
3. Historische SQL-Dateien liegen ausschließlich im separaten Compliance-Archiv und sind kein Bestandteil des Runtime-Startup-Pfads.

## Policy: keine doppelte Ownership

Schemaänderungen an den kanonisch aktiven Tabellen dürfen nur über **einen** Pfad eingeführt werden: Runtime-Migrationen im Code.

Damit werden doppelte DDL-Ausführungen und inkonsistente Deployments vermieden.
