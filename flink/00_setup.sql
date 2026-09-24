-- ============================================================================
-- StreamGuard · 00 · Workspace setup
-- ----------------------------------------------------------------------------
-- A shift-left, real-time DATA QUALITY FIREWALL on Confluent Cloud.
-- Bad data is the silent killer of streaming platforms: it flows downstream,
-- corrupts dashboards and ML features, and nobody notices until a customer
-- does. StreamGuard enforces data CONTRACTS at the edge, QUARANTINES violations
-- to a dead-letter topic, scores quality per source in real time, and FORECASTS
-- when a source will breach its quality SLA -- so you fix it before prod breaks.
--
-- Run this first. Also set in the workspace UI dropdowns:
--   Use catalog  -> default
--   Use database -> cluster_0   (your cluster name; change if different)
-- ============================================================================

USE CATALOG `default`;
USE `cluster_0`;

-- You should see `raw_events` once the feeder (or a Datagen connector) has
-- created the topic. Everything else is created by the numbered files below.
SHOW TABLES;
