-- Silver: enriched customer profiles with segmentation and upgrade propensity scoring
CREATE OR REFRESH STREAMING TABLE silver_customer_enriched
COMMENT "Customer profiles enriched with segment labels and ML-ready upgrade propensity scores for conversion targeting."
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  c.customer_id,
  UPPER(c.subscription_tier)   AS subscription_tier,
  c.monthly_revenue,
  c.account_start_date,
  c.age,
  c.location_zip,
  c.device_type,
  c.social_influence_score,
  c.data_usage_gb_monthly,
  UPPER(c.sla_tier)            AS sla_tier,
  c.churn_risk_score,
  c.last_upgrade_date,
  c.support_tickets_30d,
  -- Customer value segment
  CASE
    WHEN c.monthly_revenue >= 100 AND c.social_influence_score >= 10000 THEN 'high_value_influencer'
    WHEN c.monthly_revenue >= 100                                        THEN 'high_value'
    WHEN c.social_influence_score >= 10000                               THEN 'influencer'
    WHEN UPPER(c.subscription_tier) = 'PREMIUM'                         THEN 'premium'
    ELSE 'standard'
  END AS customer_segment,
  -- Upgrade propensity score (0-1): higher = more likely to upgrade
  CASE
    WHEN c.churn_risk_score > 0.7                                                        THEN 0.1
    WHEN c.last_upgrade_date IS NULL
         OR datediff(current_date(), c.last_upgrade_date) > 365                          THEN 0.8
    WHEN c.support_tickets_30d > 3                                                       THEN 0.9
    ELSE 0.5
  END AS upgrade_propensity_score
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_customer_profiles) c
WHERE c.customer_id IS NOT NULL;
