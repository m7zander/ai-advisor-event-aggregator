-- Legacy artifact notice: this historical migration includes an impact-table rename path that is no longer used by runtime code.
BEGIN;

ALTER TABLE IF EXISTS article_extractions RENAME TO impact_service_article_extractions;
ALTER TABLE IF EXISTS aggregated_events RENAME TO impact_service_aggregated_events;
ALTER TABLE IF EXISTS clustering_state RENAME TO impact_service_clustering_state;
ALTER TABLE IF EXISTS event_security_impacts RENAME TO impact_service_event_security_impacts;

DO $$
BEGIN
    IF to_regclass('idx_article_extractions_article_id') IS NOT NULL
       AND to_regclass('idx_impact_service_article_extractions_article_id') IS NULL THEN
        ALTER INDEX idx_article_extractions_article_id RENAME TO idx_impact_service_article_extractions_article_id;
    END IF;
    IF to_regclass('idx_article_extractions_status') IS NOT NULL
       AND to_regclass('idx_impact_service_article_extractions_status') IS NULL THEN
        ALTER INDEX idx_article_extractions_status RENAME TO idx_impact_service_article_extractions_status;
    END IF;
    IF to_regclass('idx_aggregated_events_last_seen_at') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_last_seen_at') IS NULL THEN
        ALTER INDEX idx_aggregated_events_last_seen_at RENAME TO idx_impact_service_aggregated_events_last_seen_at;
    END IF;
    IF to_regclass('idx_aggregated_events_cluster_key') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_cluster_key') IS NULL THEN
        ALTER INDEX idx_aggregated_events_cluster_key RENAME TO idx_impact_service_aggregated_events_cluster_key;
    END IF;
    IF to_regclass('idx_aggregated_events_cluster_key_unique') IS NOT NULL
       AND to_regclass('idx_impact_service_aggregated_events_cluster_key_unique') IS NULL THEN
        ALTER INDEX idx_aggregated_events_cluster_key_unique RENAME TO idx_impact_service_aggregated_events_cluster_key_unique;
    END IF;
    IF to_regclass('idx_event_security_impacts_event_id') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_event_id') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_event_id RENAME TO idx_impact_service_event_security_impacts_event_id;
    END IF;
    IF to_regclass('idx_event_security_impacts_security_code') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_security_code') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_security_code RENAME TO idx_impact_service_event_security_impacts_security_code;
    END IF;
    IF to_regclass('idx_event_security_impacts_impact_score_desc') IS NOT NULL
       AND to_regclass('idx_impact_service_event_security_impacts_impact_score_desc') IS NULL THEN
        ALTER INDEX idx_event_security_impacts_impact_score_desc RENAME TO idx_impact_service_event_security_impacts_impact_score_desc;
    END IF;
END $$;

COMMIT;
