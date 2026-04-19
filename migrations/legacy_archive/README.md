# Legacy SQL Archive Reference

## Runtime aktiv

Dieses Verzeichnis enthält keine ausführbaren SQL-Migrationen und ist kein Teil des Startup-Migrationspfads.

## Historische Referenz (Compliance)

Die historischen SQL-Dateien wurden am Cutover `runtime-schema-cutover-2026-04-19` in das externe Compliance-Archiv ausgelagert:

- `compliance://event-aggregator/sql-legacy-archive/runtime-schema-cutover-2026-04-19/`

Archivierte Dateiliste (Nachweis):

- `000001_create_article_extractions.sql`
- `000002_drop_extracted_duplicate_candidate.sql`
- `000003_create_aggregated_events.sql`
- `000004_add_extracted_industries.sql`
- `000005_add_aggregated_event_industries.sql`
- `000006_create_event_security_impacts.sql`
- `000007_drop_event_security_impacts.sql`
- `000008_rename_legacy_tables_with_service_prefix.sql`
- `000009_decommission_legacy_impact_tables.sql`
