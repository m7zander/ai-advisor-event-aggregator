<!-- This file documents migration ownership boundaries for the SQL files in this directory. -->

# Migration Ownership

## Kanonisch aktive Tabellen (verbleibender Service)

Für den verbleibenden Service sind ausschließlich folgende Tabellen kanonisch aktiv:

- `impact_service_article_extractions`
- `impact_service_aggregated_events`
- `impact_service_clustering_state`

Diese drei Tabellen werden **nur** über Runtime-Migrationen im Code gepflegt:

- `internal/repository/extraction.Repository.Migrate`

## Legacy-Artefakte (nicht mehr durch Runtime-Code verwendet)

Die folgenden Impact-Tabellen sind Legacy-Artefakte und werden nicht mehr vom Runtime-Code gelesen oder geschrieben:

- `impact_service_event_security_impacts`
- `event_security_impacts` (historischer, nicht-präfixierter Tabellenname)

Betroffene historische SQL-Dateien im Verzeichnis:

- `000006_create_event_security_impacts.sql`
- `000007_drop_event_security_impacts.sql`
- `000008_rename_legacy_tables_with_service_prefix.sql`

Diese Dateien bleiben als Historie erhalten, sind aber **nicht** der kanonische Runtime-Migrationspfad des verbleibenden Services.

## Policy: keine doppelte Ownership

Für die kanonisch aktiven Tabellen keine DDL doppelt führen:

1. Runtime-Migrationen im Code (`Repository.Migrate`), und
2. externe SQL-Dateien in `migrations/`.

Schemaänderungen an den verbleibenden Tabellen dürfen nur über **einen** Pfad eingeführt werden, um doppelte `ALTER TABLE`/`ADD COLUMN`-Ausführung bei Deployments zu vermeiden.

## Decommission für produktive Drops

Wenn produktive Drops der Legacy-Impact-Tabellen erforderlich sind, darf das nur über eine **separate Decommission-Migration** erfolgen.

Diese liegt in:

- `000009_decommission_legacy_impact_tables.sql`

Dort sind Rollout-Reihenfolge und Backout-Hinweise dokumentiert; Drops werden explizit und getrennt von bestehenden Runtime-Migrationspfaden durchgeführt.
