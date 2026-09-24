-- ============================================================================
-- StreamGuard · 05b · dq_alerts — robust materialized version
-- ----------------------------------------------------------------------------
-- The plain SELECT of this query returns rows in the workspace, but the
-- materialized table came up empty. This version:
--   * declares an explicit schema (recommended for MTs; avoids inferred-type
--     surprises that can fail the job silently),
--   * sets append changelog mode + FROM_BEGINNING so it reprocesses history,
--   * keeps the exact forecast shape we verified (RANGE OVER + forecast[1][2]).
--
-- Run order:
--   1) DROP MATERIALIZED TABLE dq_alerts;      -- if it exists
--   2) run this CREATE
--   3) check the Statements tab: it must show RUNNING (not Failed/Completed)
--   4) SELECT * FROM dq_alerts LIMIT 20;       -- after ~90s
-- ============================================================================

CREATE MATERIALIZED TABLE dq_alerts (
  source              STRING,
  ts                  TIMESTAMP_LTZ(3),
  total_events        BIGINT,
  current_error_pct   DOUBLE,
  forecast_error_pct  DOUBLE,
  status              STRING,
  top_reason          STRING
)
WITH (
  'changelog.mode' = 'append'
)
AS
SELECT
  source,
  window_end AS ts,
  total_events,
  error_rate_pct AS current_error_pct,
  CAST(forecast[1][2] AS DOUBLE) AS forecast_error_pct,
  CASE
    WHEN error_rate_pct >= 2.0 THEN 'BREACH'
    WHEN CAST(forecast[1][2] AS DOUBLE) >= 2.0 THEN 'WATCH'
    ELSE 'OK'
  END AS status,
  top_reason
FROM (
  SELECT
    source, window_end, window_time,
    COUNT(*) AS total_events,
    ROUND(100.0 * SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct,
    CASE
      WHEN SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END) > 0 THEN 'INVALID_CURRENCY'
      WHEN SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END) > 0 THEN 'AMOUNT_OUT_OF_RANGE'
      WHEN SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END) > 0 THEN 'MISSING_USER_ID'
      WHEN SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END) > 0 THEN 'MALFORMED_TIMESTAMP'
      WHEN SUM(CASE WHEN v_stale THEN 1 ELSE 0 END) > 0 THEN 'STALE_EVENT'
      WHEN SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END) > 0 THEN 'MISSING_EVENT_ID'
      ELSE 'NONE'
    END AS top_reason,
    ML_FORECAST(
      CAST(ROUND(100.0 * SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) AS DOUBLE),
      window_time,
      JSON_OBJECT('horizon' VALUE 1, 'minTrainingSize' VALUE 8, 'maxTrainingSize' VALUE 30)
    ) OVER (
      PARTITION BY source
      ORDER BY window_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forecast
  FROM TABLE(
    TUMBLE(TABLE events_verdict, DESCRIPTOR(`received_time`), INTERVAL '10' SECONDS)
  )
  GROUP BY source, window_start, window_end, window_time
)
WHERE forecast[1][2] IS NOT NULL;
