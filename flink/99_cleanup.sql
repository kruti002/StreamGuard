-- ============================================================================
-- StreamGuard · 99 · Cleanup
-- ----------------------------------------------------------------------------
-- Drops the materialized tables and views (stops their continuous statements)
-- so nothing keeps consuming your Confluent Cloud credit. Run top to bottom.
-- Then delete the Datagen connector and the cluster from the console (RUNBOOK).
-- ============================================================================

DROP MATERIALIZED TABLE dq_alerts;
DROP MATERIALIZED TABLE dq_metrics;
DROP MATERIALIZED TABLE quarantine_events;
DROP MATERIALIZED TABLE clean_events;

DROP VIEW events_verdict;
DROP VIEW events_checked;

-- raw_events is backed by the ingest topic.
-- Only drop this if YOU created it explicitly in 01 (not if a connector owns it):
-- DROP TABLE raw_events;
