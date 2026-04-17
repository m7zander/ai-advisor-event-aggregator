<!-- This file documents migration ownership boundaries for the SQL files in this directory. -->

# Migration Ownership

## Railway production path

In Railway, extraction/event schema migration is executed by the application at startup:

- `internal/repository/extraction.Repository.Migrate` owns:
  - `impact_service_article_extractions`
  - `impact_service_aggregated_events`
  - `impact_service_clustering_state`

## SQL files overlap audit

- `000001` to `000005` historically covered extraction/event schema evolution.
- Those files intentionally contain ownership/no-op notes where applicable so extraction/event columns are not added through a second migration path.

Feature SQL files (`000006` to `000008`) were removed from this repository after decommissioning in application code.

## Policy

For `impact_service_article_extractions` and `impact_service_aggregated_events`, do not introduce the same schema change in both:

1. runtime migration (`Repository.Migrate`), and
2. external SQL files in this directory.

Use exactly one ownership path for a schema change to avoid duplicate `ADD COLUMN`/DDL execution during deployments.

## Decommission note for existing production objects

If existing production DB objects from the removed feature must be actively dropped, handle that via a **separate, explicit decommission migration ticket** that plans `DROP TABLE` ordering and rollout impact control.
