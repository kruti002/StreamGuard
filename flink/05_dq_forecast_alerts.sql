-- ============================================================================
-- StreamGuard · 05 · Forecast degradation + raise the SLA alert (the twist)
-- ----------------------------------------------------------------------------
-- Counting bad records is reactive. StreamGuard is PROACTIVE: it forecasts each
-- source's error-rate trajectory and warns before the source crosses its data-
-- quality SLA (an "error budget", here 2%). That's the shift-left promise --
-- catch the degradation upstream, before it corrupts a dashboard or ML feature.
--
-- ML_FORECAST notes (Confluent Cloud):
--   * OVER window MUST be: ORDER BY <time attr> RANGE BETWEEN UNBOUNDED
--     PRECEDING AND CURRENT ROW  (a plain ORDER BY produces nothing).
--   * Output is a 2D ARRAY: forecast[1][2] is the predicted value for the first
--     horizon step; forecast[1][1] is its timestamp.
--   * We window INLINE off events_verdict so window_time is a real time
--     attribute (reading it back from the dq_metrics materialized table would
--     demote it to a plain TIMESTAMP and the forecast would emit nothing).
-- ============================================================================

CREATE MATERIALIZED TABLE dq_alerts AS
SELECT
  source,
  ts,
  total_events,
  current_error_pct,
  next_error_pct AS forecast_error_pct,
  next_error_pct AS forecast_upper_pct,   -- this ML_FORECAST returns point values
  (next_error_pct - current_error_pct) AS error_rate_slope,
  2.0 AS sla_pct,
  CASE
    WHEN current_error_pct >= 2.0 THEN 0.0
    WHEN (next_error_pct - current_error_pct) > 0.01
      THEN ((2.0 - current_error_pct) / (next_error_pct - current_error_pct)) * (10.0 / 60.0)
    ELSE CAST(NULL AS DOUBLE)
  END AS minutes_to_sla_breach,
  CASE
    WHEN current_error_pct >= 2.0 THEN 'BREACH'
    WHEN next_error_pct >= 2.0 THEN 'WATCH'
    WHEN (next_error_pct - current_error_pct) > 0.01
         AND ((2.0 - current_error_pct) / (next_error_pct - current_error_pct)) * (10.0 / 60.0) < 5
      THEN 'WATCH'
    ELSE 'OK'
  END AS status,
  top_reason
FROM (
  SELECT
    source,
    window_end AS ts,
    total_events,
    error_rate_pct AS current_error_pct,
    top_reason,
    CAST(forecast[1][2] AS DOUBLE) AS next_error_pct
  FROM (
    SELECT
      source,
      window_end,
      total_events,
      error_rate_pct,
      top_reason,
      ML_FORECAST(
        CAST(error_rate_pct AS DOUBLE),
        window_time,
        JSON_OBJECT('horizon' VALUE 1, 'minTrainingSize' VALUE 8, 'maxTrainingSize' VALUE 30)
      ) OVER (
        PARTITION BY source
        ORDER BY window_time
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS forecast
    FROM (
      SELECT
        source,
        window_start,
        window_end,
        window_time,
        COUNT(*) AS total_events,
        ROUND(100.0 * SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct,
        CASE GREATEST(
               SUM(CASE WHEN v_missing_id    THEN 1 ELSE 0 END),
               SUM(CASE WHEN v_missing_user  THEN 1 ELSE 0 END),
               SUM(CASE WHEN v_amount_range  THEN 1 ELSE 0 END),
               SUM(CASE WHEN v_bad_currency  THEN 1 ELSE 0 END),
               SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END),
               SUM(CASE WHEN v_stale         THEN 1 ELSE 0 END))
          WHEN 0 THEN 'NONE'
          ELSE
            CASE
              WHEN SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END) >= GREATEST(
                     SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_stale THEN 1 ELSE 0 END)) THEN 'INVALID_CURRENCY'
              WHEN SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END) >= GREATEST(
                     SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_stale THEN 1 ELSE 0 END)) THEN 'AMOUNT_OUT_OF_RANGE'
              WHEN SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END) >= GREATEST(
                     SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_stale THEN 1 ELSE 0 END)) THEN 'MISSING_USER_ID'
              WHEN SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END) >= GREATEST(
                     SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_stale THEN 1 ELSE 0 END)) THEN 'MALFORMED_TIMESTAMP'
              WHEN SUM(CASE WHEN v_stale THEN 1 ELSE 0 END) >= GREATEST(
                     SUM(CASE WHEN v_missing_id THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_missing_user THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_amount_range THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_currency THEN 1 ELSE 0 END),
                     SUM(CASE WHEN v_bad_timestamp THEN 1 ELSE 0 END)) THEN 'STALE_EVENT'
              ELSE 'MISSING_EVENT_ID'
            END
        END AS top_reason
      FROM TABLE(
        TUMBLE(TABLE events_verdict, DESCRIPTOR(`received_time`), INTERVAL '10' SECONDS)
      )
      GROUP BY source, window_start, window_end, window_time
    )
  )
  WHERE forecast[1][2] IS NOT NULL
) t;

-- The live source-health board: one row per source (its latest window).
SELECT source, status, current_error_pct, forecast_error_pct,
       ROUND(minutes_to_sla_breach, 1) AS eta_min, top_reason
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY source ORDER BY ts DESC) AS rn
  FROM dq_alerts
)
WHERE rn = 1;
