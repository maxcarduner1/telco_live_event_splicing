-- Bronze: active network slice configurations from slice orchestrator
CREATE OR REFRESH STREAMING TABLE bronze_network_slices
COMMENT "Active network slice configurations and performance metrics from the TelcoMax slice orchestrator. Includes autonomous provisioning surge during USMNT match."
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  slice_id,
  customer_id,
  tower_id,
  slice_type,
  bandwidth_allocated_mbps,
  latency_guarantee_ms,
  created_timestamp,
  expires_timestamp,
  status,
  revenue_per_hour,
  current_timestamp() AS ingested_at
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_network_slices);
