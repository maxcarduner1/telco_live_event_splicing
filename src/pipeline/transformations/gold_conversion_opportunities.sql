-- Gold: real-time B2B upsell opportunity scoring
-- Identifies business customers whose active slices are approaching or breaching
-- their contracted bandwidth limit during the live event.
-- Renamed conceptually to "upsell opportunities" but kept as gold_conversion_opportunities
-- to preserve pipeline dependency references.
CREATE OR REFRESH MATERIALIZED VIEW gold_conversion_opportunities
COMMENT "B2B customers near or over their contracted bandwidth limit during the live event. Scored for upsell urgency and contract expansion revenue opportunity."
TBLPROPERTIES ("quality" = "gold")
AS
WITH latest_congestion AS (
  -- Use the most recent 5-min window per tower
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
  -- Latest utilization snapshot per customer (highest-utilization active slice)
  SELECT
    customer_id,
    tower_id,
    slice_id,
    contracted_bandwidth_mbps,
    current_bandwidth_mbps,
    utilization_pct,
    latency_guarantee_ms,
    revenue_per_hour,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY utilization_pct DESC) AS rn
  FROM cmegdemos_catalog.dynamic_slicing_live_event.bronze_network_slices
  WHERE status = 'active'
)
SELECT
  c.customer_id,
  c.company_name,
  c.customer_type,
  c.customer_segment,
  c.contract_tier,
  c.monthly_contract_value,
  c.contracted_bandwidth_mbps,
  c.contracted_latency_ms,
  c.upsell_propensity_score,
  c.monthly_upsell_opportunity_usd,
  c.churn_risk_score,
  c.contract_renewal_months,
  c.account_manager_id,
  s.slice_id,
  s.tower_id,
  s.current_bandwidth_mbps,
  s.utilization_pct,
  lc.window_start,
  lc.avg_congestion_score,
  lc.bandwidth_trend_15min,
  lc.congestion_predicted_15min,
  lc.near_event_flag,
  -- Bandwidth breach risk level
  CASE
    WHEN s.utilization_pct >= 100 THEN 'breach'
    WHEN s.utilization_pct >=  90 THEN 'critical'
    WHEN s.utilization_pct >=  85 THEN 'warning'
    ELSE 'watch'
  END AS breach_risk_level,
  -- Composite upsell score (0-1): urgency × propensity
  ROUND(
    (c.upsell_propensity_score * 0.45 +
     -- Utilization pressure (85%→ full weight)
     LEAST(1.0, GREATEST(0.0, (s.utilization_pct - 75.0) / 25.0)) * 0.35 +
     -- Congestion prediction bonus
     CASE WHEN lc.congestion_predicted_15min = 1 THEN 0.20 ELSE 0.0 END),
    4
  ) AS conversion_score,
  -- Upsell: additional monthly revenue from 50% bandwidth expansion
  c.monthly_upsell_opportunity_usd    AS monthly_revenue_opportunity,
  current_timestamp()                 AS scored_at
FROM LIVE.silver_customer_enriched c
JOIN active_slices s ON c.customer_id = s.customer_id AND s.rn = 1
JOIN latest_congestion lc ON s.tower_id = lc.tower_id AND lc.rn = 1
WHERE s.utilization_pct >= 85
   OR (s.utilization_pct >= 75 AND lc.congestion_predicted_15min = 1);
