-- Legacy artifact notice: this historical migration created an impact table that is no longer used by runtime code.
BEGIN;

CREATE TABLE IF NOT EXISTS impact_service_event_security_impacts (
    event_id TEXT NOT NULL,
    security_code TEXT NOT NULL,
    isin TEXT NOT NULL,
    security_name TEXT NOT NULL,
    impact_direction TEXT NOT NULL,
    impact_score DOUBLE PRECISION NOT NULL,
    impact_confidence DOUBLE PRECISION NOT NULL,
    geo_match_score DOUBLE PRECISION NOT NULL,
    country_match_score DOUBLE PRECISION NOT NULL,
    sector_match_score DOUBLE PRECISION NOT NULL,
    event_type_match_score DOUBLE PRECISION NOT NULL,
    profile_confidence DOUBLE PRECISION NOT NULL,
    rule_version TEXT NOT NULL,
    explanation_codes JSONB NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, security_code, rule_version)
);

CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_event_id
    ON impact_service_event_security_impacts (event_id);

CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_security_code
    ON impact_service_event_security_impacts (security_code);

CREATE INDEX IF NOT EXISTS idx_impact_service_event_security_impacts_impact_score_desc
    ON impact_service_event_security_impacts (impact_score DESC);

COMMIT;
