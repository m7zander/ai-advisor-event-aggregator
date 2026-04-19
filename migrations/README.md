# Migrations (Runtime Scope)

Dieses Verzeichnis enthält **keine** SQL-Dateien für die Laufzeit.
Die SQL-Dateien aus älteren Repository-Ständen wurden aus dem Service entfernt und sind aus Compliance-Gründen separat archiviert.

Der Ordner `legacy_archive` ist nur dokumentarisch und **nicht Teil des Runtime-Migrationspfads**.

Aktiver Runtime-Migrationspfad im Service-Code:
- `internal/repository/extraction/repository.go`
- Methode: `(*Repository).Migrate`

`(*Repository).Migrate` ist die verbindliche Laufzeit-Entry-Point-Implementierung für Schema-Initialisierung und -Änderungen.
