-- ============================================================================
-- StreamGuard · 04 · Per-source quality metrics (the DQ time series)
-- ----------------------------------------------------------------------------
-- Tumble every source's events into 10-second windows and compute a live
-- quality scorecard per source:
--   total, bad, error_rate_pct
--   individual failure-mode rates (null/user, range, currency, timestamp, stale)
--   avg_freshness_sec (how stale the source is running)
--
-- error_rate_pct per window is the numeric time series ML_FORECAST consumes in
-- the next step. Windowing on event time (via TUMBLE + $rowtime) means late /
-- out-of-order events are handled correctly by watermarks -- which matters,
-- because a degrading source often ALSO gets laggy.
-- ============================================================================

CREATE MATERIALIZED TABLE dq_metrics AS
SELECT
  source,
  window_start,
  window_end,
  window_time,
  COUNT(*)                                              AS total_events,
  SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END)         AS bad_events,
  ROUND(100.0 * SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct,
  SUM(CASE WHEN v_missing_id   THEN 1 ELSE 0 END)       AS cnt_missing_id,
  SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END)       AS cnt_missing_user,
  SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END)       AS cnt_amount_range,
  SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END)       AS cnt_bad_currency,
  SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END)      AS cnt_bad_timestamp,
  SUM(CASE WHEN v_stale        THEN 1 ELSE 0 END)       AS cnt_stale,
  ROUND(AVG(CAST(freshness_sec AS DOUBLE)), 1)          AS avg_freshness_sec
FROM TABLE(
  TUMBLE(TABLE events_verdict, DESCRIPTOR(`received_time`), INTERVAL '10' SECONDS)
)
GROUP BY source, window_start, window_end, window_time;

-- Live scorecard per source (latest window).
SELECT source, total_events, bad_events, error_rate_pct, avg_freshness_sec, window_end
FROM dq_metrics
ORDER BY window_end DESC
LIMIT 25;
