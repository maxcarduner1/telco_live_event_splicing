-- Bronze: raw cell tower telemetry ingestion
-- Reads from the source table populated by the data generation job
CREATE OR REFRESH STREAMING TABLE bronze_cell_tower_telemetry
COMMENT "Raw cell tower telemetry streamed from TelcoMax Network Management System. 50 towers across Seattle metro at 1-minute intervals."
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  tower_id,
  timestamp,
  latitude,
  longitude,
  active_connections,
  bandwidth_utilization_pct,
  signal_strength_dbm,
  latency_ms,
  packet_loss_pct,
  throughput_mbps,
  error_rate_pct,
  temperature_celsius,
  power_consumption_watts,
  current_timestamp() AS ingested_at
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_cell_tower_telemetry);
