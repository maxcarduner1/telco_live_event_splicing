-- Gold: real-time customer conversion opportunity scoring
-- Materialized view: joins gold congestion features with customer profiles and network slices
CREATE OR REFRESH MATERIALIZED VIEW gold_conversion_opportunities
COMMENT "Scoring of high-value customers near congested towers. Targets for autonomous premium plan upgrade offers during live events."
TBLPROPERTIES ("quality" = "gold")
AS
WITH latest_congestion AS (
  -- Use the most recent 5-min window per tower to score current state
  SELECT
    tower_id,
    avg_congestion_score,
    bandwidth_trend_15min,
    congestion_predicted_15min,
    near_event_flag,
    window_start,
    ROW_NUMBER() OVER (PARTITION BY tower_id ORDER BY window_start DESC) AS rn
  FROM LIVE.gold_congestion_features
  WHERE congestion_predicted_15min = 1 OR avg_congestion_score > 60
),
active_slices AS (
  SELECT customer_id, tower_id, slice_id
  FROM cmegdemos_catalog.dynamic_slicing_live_event.bronze_network_slices
  WHERE status IN ('active', 'provisioning')
)
SELECT
  c.customer_id,
  c.customer_segment,
  c.subscription_tier,
  c.monthly_revenue,
  c.upgrade_propensity_score,
  c.social_influence_score,
  c.device_type,
  s.slice_id,
  s.tower_id,
  lc.window_start,
  lc.avg_congestion_score,
  lc.bandwidth_trend_15min,
  lc.congestion_predicted_15min,
  lc.near_event_flag,
  -- Composite conversion score (0-1)
  ROUND(
    (c.upgrade_propensity_score * 0.40 +
     CASE
       WHEN c.customer_segment IN ('high_value_influencer', 'influencer') THEN 0.30
       ELSE 0.10
     END +
     CASE WHEN lc.congestion_predicted_15min = 1 THEN 0.30 ELSE 0.0 END),
    4
  ) AS conversion_score,
  CASE c.subscription_tier
    WHEN 'BASIC'   THEN 25.0
    WHEN 'PREMIUM' THEN 50.0
    ELSE 0.0
  END AS monthly_revenue_opportunity,
  current_timestamp() AS scored_at
FROM LIVE.silver_customer_enriched c
JOIN active_slices s ON c.customer_id = s.customer_id
JOIN latest_congestion lc ON s.tower_id = lc.tower_id AND lc.rn = 1
WHERE c.subscription_tier IN ('BASIC', 'PREMIUM');
