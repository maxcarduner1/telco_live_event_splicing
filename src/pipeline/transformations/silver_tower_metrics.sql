-- Silver: cleaned and standardized tower metrics with congestion scoring
-- Adds composite congestion_score used by NWDAF models
CREATE OR REFRESH STREAMING TABLE silver_tower_metrics
COMMENT "Cleaned tower telemetry with validated ranges, null handling, and composite congestion scoring for NWDAF prediction."
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  tower_id,
  timestamp,
  latitude,
  longitude,
  GREATEST(0, active_connections)                                     AS active_connections,
  LEAST(100, GREATEST(0, bandwidth_utilization_pct))                 AS bandwidth_utilization_pct,
  CASE
    WHEN signal_strength_dbm BETWEEN -120 AND -30 THEN signal_strength_dbm
    ELSE NULL
  END                                                                  AS signal_strength_dbm,
  GREATEST(0, latency_ms)                                             AS latency_ms,
  LEAST(100, GREATEST(0, packet_loss_pct))                           AS packet_loss_pct,
  GREATEST(0, throughput_mbps)                                        AS throughput_mbps,
  LEAST(100, GREATEST(0, error_rate_pct))                            AS error_rate_pct,
  temperature_celsius,
  power_consumption_watts,
  -- Composite congestion score (0-100): weighted combination of key KPIs
  ROUND(
    (LEAST(100, GREATEST(0, bandwidth_utilization_pct)) * 0.40 +
     LEAST(100, GREATEST(0, packet_loss_pct))           * 0.30 +
     LEAST(100, GREATEST(0, latency_ms) / 100)          * 0.20 +
     LEAST(100, GREATEST(0, error_rate_pct))             * 0.10),
    2
  )                                                                    AS congestion_score
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_cell_tower_telemetry)
WHERE tower_id IS NOT NULL
  AND timestamp IS NOT NULL
  AND bandwidth_utilization_pct IS NOT NULL;
