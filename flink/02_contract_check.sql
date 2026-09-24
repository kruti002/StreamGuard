-- ============================================================================
-- StreamGuard · 02 · Contract enforcement (the firewall logic)
-- ----------------------------------------------------------------------------
-- This is the data CONTRACT expressed as executable rules. For every raw event
-- we evaluate each rule and emit:
--   is_valid       - did it pass ALL rules?
--   violations     - array of the specific rules it broke (the DLQ reason)
--   parsed_event_ts - the source timestamp parsed to a real TIMESTAMP (or NULL)
--   freshness_sec  - how stale the event was when we received it
--
-- Expressing the contract as a view (rather than only in Schema Registry) lets
-- us capture SEMANTIC rules Schema Registry can't (ranges, enums, freshness),
-- and-crucially-keep the bad records flowing so we can measure and quarantine
-- them instead of silently dropping them. Schema Registry still guards the wire
-- format; this guards the meaning.
-- ============================================================================

CREATE VIEW events_checked AS
SELECT
  event_id,
  source,
  amount,
  currency,
  user_id,
  event_ts,
  `$rowtime` AS received_time,
  -- parse the source timestamp; NULL if malformed
  TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) AS parsed_event_ts,
  -- freshness: seconds between the source event time and when we got it
  CASE
    WHEN TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) IS NOT NULL
      THEN TIMESTAMPDIFF(SECOND, TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)), `$rowtime`)
    ELSE CAST(NULL AS BIGINT)
  END AS freshness_sec,
  -- individual rule checks (TRUE = the rule was VIOLATED)
  (event_id IS NULL OR CHAR_LENGTH(event_id) = 0)                       AS v_missing_id,
  (user_id IS NULL OR CHAR_LENGTH(user_id) = 0)                         AS v_missing_user,
  (amount IS NULL OR amount < 0 OR amount > 100000)                     AS v_amount_range,
  (currency IS NULL OR currency NOT IN ('USD','EUR','GBP','JPY','INR')) AS v_bad_currency,
  (TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) IS NULL)                      AS v_bad_timestamp,
  -- stale: source time more than 5 minutes behind receipt
  (TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) IS NOT NULL
     AND TIMESTAMPDIFF(SECOND, TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)), `$rowtime`) > 300) AS v_stale,
  -- collect the human-readable reasons into an array (the DLQ payload)
  ARRAY_REMOVE(ARRAY[
    CASE WHEN (event_id IS NULL OR CHAR_LENGTH(event_id) = 0) THEN 'MISSING_EVENT_ID' END,
    CASE WHEN (user_id IS NULL OR CHAR_LENGTH(user_id) = 0) THEN 'MISSING_USER_ID' END,
    CASE WHEN (amount IS NULL OR amount < 0 OR amount > 100000) THEN 'AMOUNT_OUT_OF_RANGE' END,
    CASE WHEN (currency IS NULL OR currency NOT IN ('USD','EUR','GBP','JPY','INR')) THEN 'INVALID_CURRENCY' END,
    CASE WHEN (TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) IS NULL) THEN 'MALFORMED_TIMESTAMP' END,
    CASE WHEN (TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)) IS NOT NULL
                AND TIMESTAMPDIFF(SECOND, TRY_CAST(event_ts AS TIMESTAMP_LTZ(3)), `$rowtime`) > 300)
         THEN 'STALE_EVENT' END
  ], CAST(NULL AS STRING)) AS violations
FROM raw_events;

-- Add the overall verdict on top of the per-rule checks.
CREATE VIEW events_verdict AS
SELECT
  *,
  CARDINALITY(violations) = 0 AS is_valid
FROM events_checked;

-- Spot-check: see valid vs violating events side by side.
SELECT source, event_id, amount, currency, is_valid, violations
FROM events_verdict
ORDER BY received_time DESC
LIMIT 25;
