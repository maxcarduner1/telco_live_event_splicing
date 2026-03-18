-- Silver: event-tower geospatial proximity mapping using Haversine approximation
-- Materialized view: towers within 5 miles of each event venue
CREATE OR REFRESH MATERIALIZED VIEW silver_event_tower_proximity
COMMENT "Geospatial mapping of towers within 5-mile radius of event venues. Used to identify towers that will experience traffic spikes during events."
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  e.event_id,
  e.event_name,
  e.venue_name,
  e.event_start_time,
  e.event_end_time,
  e.expected_attendance,
  e.traffic_multiplier,
  t.tower_id,
  t.latitude  AS tower_latitude,
  t.longitude AS tower_longitude,
  -- Haversine approximation (miles)
  ROUND(
    SQRT(
      POW(69.1 * (e.venue_latitude - t.latitude), 2) +
      POW(69.1 * (e.venue_longitude - t.longitude) * COS(e.venue_latitude / 57.3), 2)
    ),
    3
  ) AS distance_miles
FROM cmegdemos_catalog.dynamic_slicing_live_event.bronze_event_calendar e
CROSS JOIN (
  SELECT DISTINCT tower_id, latitude, longitude
  FROM cmegdemos_catalog.dynamic_slicing_live_event.bronze_cell_tower_telemetry
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
) t
WHERE SQRT(
  POW(69.1 * (e.venue_latitude - t.latitude), 2) +
  POW(69.1 * (e.venue_longitude - t.longitude) * COS(e.venue_latitude / 57.3), 2)
) <= 5.0;
