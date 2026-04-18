BEGIN;

-- Decommission migration: remove legacy impact tables that are no longer used by runtime code.
--
-- Rollout-Reihenfolge (Production):
--   1) Deploy Runtime ohne Referenzen auf Impact-Tabellen (bereits erfüllt).
--   2) Verifizieren, dass keine Reads/Writes auf impact_service_event_security_impacts stattfinden.
--   3) Diese Migration in einem separaten Change-Fenster ausrollen.
--
-- Backout-Hinweise:
--   - Ein Daten-Backout erfordert Restore aus Backup/Snapshot vor Migration.
--   - Ein reines Code-Rollback stellt gedroppte Tabellen nicht wieder her.
--   - Bei Unsicherheit Migration abbrechen und zuerst Snapshot-Strategie bestätigen.

DROP TABLE IF EXISTS impact_service_event_security_impacts;
DROP TABLE IF EXISTS event_security_impacts;

COMMIT;
