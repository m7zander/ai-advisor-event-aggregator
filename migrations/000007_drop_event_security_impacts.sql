BEGIN;

-- Safety: this decommission migration only targets the legacy pre-prefix table name.
-- Never drop the canonical impact_service_event_security_impacts table from active schema flow.
DROP TABLE IF EXISTS event_security_impacts;

COMMIT;
