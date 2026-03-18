-- Bronze: major events calendar ingestion from event management APIs
CREATE OR REFRESH STREAMING TABLE bronze_event_calendar
COMMENT "Major events driving network traffic spikes. Includes USMNT match, Taylor Swift Eras Tour, Seahawks games."
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  event_id,
  event_name,
  venue_name,
  venue_latitude,
  venue_longitude,
  event_start_time,
  event_end_time,
  expected_attendance,
  event_type,
  traffic_multiplier,
  current_timestamp() AS ingested_at
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_event_calendar);
