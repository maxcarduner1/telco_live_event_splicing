-- Gold: revenue analytics aggregations for Genie Space and dashboard
-- Shows projected ARR from conversion opportunities by segment and hour
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_analytics
COMMENT "Hourly revenue opportunity aggregations by customer segment. Powers the Genie Space natural language analytics and executive KPI dashboard."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  DATE(scored_at)                           AS analysis_date,
  EXTRACT(HOUR FROM scored_at)              AS hour_of_day,
  customer_segment,
  COUNT(*)                                  AS total_opportunities,
  COUNT(CASE WHEN conversion_score > 0.7 THEN 1 END)
                                            AS high_conversion_opportunities,
  ROUND(AVG(conversion_score), 4)           AS avg_conversion_score,
  ROUND(SUM(monthly_revenue_opportunity), 2)
                                            AS total_revenue_opportunity,
  ROUND(SUM(CASE WHEN conversion_score > 0.7 THEN monthly_revenue_opportunity ELSE 0 END), 2)
                                            AS high_confidence_revenue_opportunity,
  -- Simulated conversion metrics at 47% rate (validated from USMNT match)
  ROUND(COUNT(CASE WHEN conversion_score > 0.7 THEN 1 END) * 0.47, 0)
                                            AS projected_conversions,
  ROUND(SUM(CASE WHEN conversion_score > 0.7 THEN monthly_revenue_opportunity ELSE 0 END) * 0.47 * 12, 2)
                                            AS projected_annual_revenue,
  -- Influencer amplification: social reach of converted customers
  SUM(CASE WHEN conversion_score > 0.7 THEN social_influence_score ELSE 0 END)
                                            AS total_social_reach_on_conversion
FROM LIVE.gold_conversion_opportunities
GROUP BY
  DATE(scored_at),
  EXTRACT(HOUR FROM scored_at),
  customer_segment;
