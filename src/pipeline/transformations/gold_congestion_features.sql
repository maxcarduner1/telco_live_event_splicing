-- Gold: NWDAF congestion prediction features with 5-minute windowed aggregations
-- Materialized view: supports LAG and EXISTS for full NWDAF feature set
CREATE OR REFRESH MATERIALIZED VIEW gold_congestion_features
COMMENT "NWDAF-ready features for 15-minute-ahead congestion prediction. 5-minute windows with bandwidth/connection trends and event proximity flags."
TBLPROPERTIES ("quality" = "gold")
AS
WITH windowed AS (
  SELECT
    tower_id,
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(timestamp) / 300) * 300)     AS window_start,
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(timestamp) / 300) * 300 + 300) AS window_end,
    ROUND(AVG(bandwidth_utilization_pct), 2)  AS avg_bandwidth_util,
    ROUND(MAX(bandwidth_utilization_pct), 2)  AS peak_bandwidth_util,
    ROUND(AVG(active_connections), 0)         AS avg_connections,
    MAX(active_connections)                   AS peak_connections,
    ROUND(AVG(congestion_score), 2)           AS avg_congestion_score,
    ROUND(AVG(latency_ms), 2)                 AS avg_latency_ms,
    ROUND(AVG(packet_loss_pct), 4)            AS avg_packet_loss_pct,
    EXTRACT(HOUR FROM MIN(timestamp))         AS hour_of_day,
    EXTRACT(DOW  FROM MIN(timestamp))         AS day_of_week
  FROM LIVE.silver_tower_metrics
  GROUP BY tower_id, FLOOR(UNIX_TIMESTAMP(timestamp) / 300)
),
with_trends AS (
  SELECT
    *,
    ROUND(
      COALESCE(
        avg_bandwidth_util - LAG(avg_bandwidth_util, 1) OVER (
          PARTITION BY tower_id ORDER BY window_start
        ),
        0
      ), 2
    ) AS bandwidth_trend_15min,
    ROUND(
      COALESCE(
        avg_connections - LAG(avg_connections, 1) OVER (
          PARTITION BY tower_id ORDER BY window_start
        ),
        0
      ), 1
    ) AS connection_trend_15min
  FROM windowed
),
with_event_flag AS (
  SELECT
    w.*,
    COALESCE(ep.near_event, 0) AS near_event_flag
  FROM with_trends w
  LEFT JOIN (
    SELECT DISTINCT tower_id, 1 AS near_event
    FROM LIVE.silver_event_tower_proximity
  ) ep ON w.tower_id = ep.tower_id
)
SELECT
  tower_id,
  window_start,
  window_end,
  avg_bandwidth_util,
  peak_bandwidth_util,
  CAST(avg_connections AS BIGINT)   AS avg_connections,
  peak_connections,
  avg_congestion_score,
  avg_latency_ms,
  avg_packet_loss_pct,
  bandwidth_trend_15min,
  connection_trend_15min,
  near_event_flag,
  CAST(hour_of_day AS INT)          AS hour_of_day,
  CAST(day_of_week AS INT)          AS day_of_week,
  CASE
    WHEN avg_bandwidth_util > 75 AND bandwidth_trend_15min > 10 THEN 1
    WHEN avg_bandwidth_util > 85                                 THEN 1
    ELSE 0
  END AS congestion_predicted_15min
FROM with_event_flag;
