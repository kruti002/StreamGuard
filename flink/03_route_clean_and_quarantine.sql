-- ============================================================================
-- StreamGuard · 03 · Route: clean topic + dead-letter (quarantine) topic
-- ----------------------------------------------------------------------------
-- The firewall's two exits:
--   clean_events      - passed the contract; safe for downstream consumers,
--                       dashboards, and ML feature pipelines.
--   quarantine_events - the DEAD-LETTER queue: every rejected record, kept with
--                       the exact rule(s) it broke so an on-call engineer can
--                       triage instead of guessing. We NEVER silently drop.
--
-- Keeping a DLQ (rather than dropping) is the pattern serious streaming teams
-- use: bad data is preserved for replay/repair once the upstream bug is fixed.
-- ============================================================================

-- Trusted lane: only contract-valid events move forward.
CREATE MATERIALIZED TABLE clean_events (
  event_id  STRING,
  source    STRING,
  amount    DOUBLE,
  currency  STRING,
  user_id   STRING,
  event_time TIMESTAMP_LTZ(3)
) AS
SELECT event_id, source, amount, currency, user_id, parsed_event_ts AS event_time
FROM events_verdict
WHERE is_valid;

-- Dead-letter lane: everything that failed, with the reason(s) attached.
CREATE MATERIALIZED TABLE quarantine_events (
  event_id     STRING,
  source       STRING,
  amount       DOUBLE,
  currency     STRING,
  user_id      STRING,
  raw_event_ts STRING,
  violations   ARRAY<STRING>,
  quarantined_at TIMESTAMP_LTZ(3)
) AS
SELECT event_id, source, amount, currency, user_id, event_ts AS raw_event_ts,
       violations, received_time AS quarantined_at
FROM events_verdict
WHERE NOT is_valid;

-- Live DLQ feed for the dashboard.
SELECT source, event_id, violations, quarantined_at
FROM quarantine_events
ORDER BY quarantined_at DESC
LIMIT 25;
