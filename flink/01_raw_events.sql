-- ============================================================================
-- StreamGuard · 01 · Raw ingest stream (the untrusted edge)
-- ----------------------------------------------------------------------------
-- `raw_events` is the untrusted firehose: order/payment-like events arriving
-- from multiple upstream SOURCES. We intentionally read every field as nullable
-- and permissive here, because the whole point is that we DON'T trust it yet.
-- The contract is enforced in the next step, not by the ingest schema.
--
-- The event carries its own `event_ts` (source-stamped) so we can measure
-- FRESHNESS (how stale a source is) independently of when Kafka received it.
-- We keep the Kafka record time as `$rowtime` for event-time windowing.
--
-- Canonical event shape the feeder emits:
--   {
--     "event_id":   "e-8f3...",       -- should be non-null, unique-ish
--     "source":     "checkout-svc",   -- which upstream produced it
--     "amount":     42.50,            -- must be >= 0 and <= 100000
--     "currency":   "USD",            -- must be one of a known set
--     "user_id":    "u-1024",         -- should be non-null
--     "event_ts":   "2026-09-22T18:03:11.000Z"  -- source event time
--   }
-- Bad records violate one or more of those rules on purpose.
-- ============================================================================

-- IMPORTANT (Confluent Cloud Flink):
-- Confluent Flink only reads registry-backed formats (JSON_SR / Avro / Protobuf),
-- NOT plain 'json'. The feeder registers a JSON Schema for `raw_events-value`
-- (see feeder/feeder.js), so once the feeder has produced at least once, Flink
-- AUTO-INFERS this table from the topic + its schema. You usually don't need to
-- CREATE it at all.
--
-- Step 1: start the feeder first (it registers the schema + produces).
-- Step 2: then just query the inferred table:

SELECT source, event_id, amount, currency, user_id, event_ts, `$rowtime` AS received
FROM raw_events
ORDER BY `$rowtime` DESC
LIMIT 20;

-- If `raw_events` was created earlier WITHOUT a schema and Flink shows no
-- columns, drop the stale Flink table metadata and re-infer (this does not
-- delete the Kafka topic or its data):
--
--   DROP TABLE IF EXISTS raw_events;
--
-- then re-run the SELECT above after the feeder has produced with its schema.
--
-- Fallback only (rarely needed): declare the columns explicitly against the
-- registry format. Run this INSTEAD of relying on inference if the inferred
-- table doesn't expose the fields:
--
--   CREATE TABLE raw_events (
--     event_id   STRING,
--     source     STRING,
--     amount     DOUBLE,
--     currency   STRING,
--     user_id    STRING,
--     event_ts   STRING,
--     ingest_ts  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
--   ) WITH (
--     'value.format' = 'json-registry',
--     'changelog.mode' = 'append'
--   );
