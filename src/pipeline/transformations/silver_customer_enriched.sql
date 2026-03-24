-- Silver: enriched B2B customer profiles with segmentation and upsell propensity scoring
CREATE OR REFRESH STREAMING TABLE silver_customer_enriched
COMMENT "B2B stadium business customers enriched with segment labels and upsell propensity scores. Powers bandwidth breach detection and contract expansion targeting."
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  c.customer_id,
  c.company_name,
  c.customer_type,
  UPPER(c.contract_tier)              AS contract_tier,
  c.monthly_contract_value,
  c.contracted_bandwidth_mbps,
  c.contracted_latency_ms,
  UPPER(c.sla_tier)                   AS sla_tier,
  c.churn_risk_score,
  c.contract_start_date,
  c.contract_renewal_months,
  c.support_tickets_30d,
  c.peak_event_utilization_pct,
  c.account_manager_id,
  -- B2B customer segment based on type and contract value
  CASE
    WHEN c.customer_type = 'broadcaster'        THEN 'media_rights'
    WHEN c.customer_type = 'public_safety'      THEN 'critical_services'
    WHEN c.customer_type = 'venue_operator'     THEN 'venue_ops'
    WHEN c.customer_type = 'payment_processor'  THEN 'payments_ticketing'
    WHEN c.customer_type = 'team_sponsor'       THEN 'teams_sponsors'
    ELSE 'other'
  END AS customer_segment,
  -- Upsell propensity (0-1): higher = more likely to accept capacity expansion offer
  -- Driven by contract renewal urgency, bandwidth headroom usage, and support history
  ROUND(
    CASE
      WHEN c.churn_risk_score > 0.70                           THEN 0.15  -- already churning, protect first
      WHEN c.contract_renewal_months <= 3
           AND c.peak_event_utilization_pct >= 0.80            THEN 0.90  -- renewal close + high usage
      WHEN c.contract_renewal_months <= 6
           AND c.peak_event_utilization_pct >= 0.75            THEN 0.80
      WHEN c.support_tickets_30d >= 3                          THEN 0.85  -- friction signals upsell need
      WHEN c.peak_event_utilization_pct >= 0.85               THEN 0.75
      WHEN c.peak_event_utilization_pct >= 0.70               THEN 0.60
      ELSE 0.40
    END,
    4
  ) AS upsell_propensity_score,
  -- Monthly upsell revenue opportunity: expand bandwidth by 50% of contracted amount
  ROUND(c.contracted_bandwidth_mbps * 0.50 * 0.008 * 1000, 2)
    AS monthly_upsell_opportunity_usd
FROM STREAM(cmegdemos_catalog.dynamic_slicing_live_event.bronze_customer_profiles) c
WHERE c.customer_id IS NOT NULL;
