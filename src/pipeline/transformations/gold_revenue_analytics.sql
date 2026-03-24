-- Gold: B2B revenue analytics aggregations for Genie Space and dashboard
-- Shows upsell opportunities and churn-prevention value by customer segment and hour
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_analytics
COMMENT "Hourly B2B upsell opportunity and churn-prevention ARR aggregations by customer segment. Powers the Genie Space natural language analytics and executive KPI dashboard."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  DATE(scored_at)                           AS analysis_date,
  EXTRACT(HOUR FROM scored_at)              AS hour_of_day,
  customer_segment,
  COUNT(*)                                  AS total_customers_at_risk,
  -- Breach severity breakdown
  COUNT(CASE WHEN breach_risk_level = 'breach'   THEN 1 END) AS breach_count,
  COUNT(CASE WHEN breach_risk_level = 'critical' THEN 1 END) AS critical_count,
  COUNT(CASE WHEN breach_risk_level = 'warning'  THEN 1 END) AS warning_count,
  -- Upsell pipeline
  COUNT(CASE WHEN conversion_score > 0.70 THEN 1 END)
                                            AS high_upsell_opportunities,
  ROUND(AVG(conversion_score), 4)           AS avg_upsell_score,
  ROUND(SUM(monthly_revenue_opportunity), 2)
                                            AS total_monthly_upsell_opportunity,
  ROUND(SUM(CASE WHEN conversion_score > 0.70 THEN monthly_revenue_opportunity ELSE 0 END), 2)
                                            AS high_confidence_monthly_upsell,
  -- Projected upsell ARR (62% acceptance rate for B2B upsell offers during events)
  ROUND(SUM(CASE WHEN conversion_score > 0.70 THEN monthly_revenue_opportunity ELSE 0 END) * 0.62 * 12, 2)
                                            AS projected_upsell_arr,
  -- Churn prevention: ARR at risk from accounts with high churn + congestion
  ROUND(SUM(CASE WHEN churn_risk_score > 0.25 AND conversion_score > 0.60
                 THEN monthly_contract_value * 12 ELSE 0 END), 2)
                                            AS arr_at_churn_risk,
  -- Average utilization across flagged customers
  ROUND(AVG(utilization_pct), 2)            AS avg_utilization_pct,
  ROUND(MAX(utilization_pct), 2)            AS peak_utilization_pct
FROM LIVE.gold_conversion_opportunities
GROUP BY
  DATE(scored_at),
  EXTRACT(HOUR FROM scored_at),
  customer_segment;
