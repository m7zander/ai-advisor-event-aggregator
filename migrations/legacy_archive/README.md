# Legacy Migration Archive Reference

Dieser Ordner enthält keine ausführbaren SQL-Dateien.
Er dient ausschließlich als Referenzhinweis, dass historische Migrationen außerhalb dieses Service-Repositories archiviert wurden.

Wichtig:
- Keine SQL-Dateien im Service-Repo unter `migrations/legacy_archive`
- Keine Aufnahme in Runtime-Migrationspfade
- Runtime-Migrationen laufen ausschließlich über `(*Repository).Migrate` in `internal/repository/extraction/repository.go`
