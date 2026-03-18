-- Bronze: customer profiles ingestion from CRM/BSS systems
CREATE OR REFRESH STREAMING TABLE bronze_customer_profiles
COMMENT "Customer subscription details, demographics, and behavioral scoring from TelcoMax CRM. 500K subscribers."
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  customer_id,
  subscription_tier,
  monthly_revenue,
  account_start_date,
  age,
  location_zip,
  device_type,
  social_influence_score,
  data_usage_gb_monthly,
  sla_tier,
  churn_risk_score,
  last_upgrade_date,
  support_tickets_30d,
  current_timestamp() AS ingested_at
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_customer_profiles);
