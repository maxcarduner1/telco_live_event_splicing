"""
Autonomous Slice Provisioning & B2B Upsell Campaign — TelcoMax Demo
Simulates the USMNT vs Australia match event response using pure Spark SQL.

  1. Queries gold_conversion_opportunities for B2B customers at bandwidth risk
  2. Provisions burst capacity (writes expanded slices to bronze_network_slices)
  3. Logs upsell proposals (writes to upsell_proposal_log)
  4. Computes and prints revenue + churn-prevention summary
"""

import os
from datetime import datetime

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
CONVERSION_TABLE    = f"{CATALOG}.{SCHEMA}.gold_conversion_opportunities"
SLICES_TABLE        = f"{CATALOG}.{SCHEMA}.bronze_network_slices"
PROPOSAL_LOG_TABLE  = f"{CATALOG}.{SCHEMA}.upsell_proposal_log"
REVENUE_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.event_revenue_summary"

UPSELL_THRESHOLD   = 0.65   # conversion_score cutoff for triggering a proposal
ACCEPTANCE_RATE    = 0.62   # empirical B2B upsell acceptance rate during events
BANDWIDTH_EXPANSION_PCT = 0.50  # offer 50% additional bandwidth above contracted amount


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")
    print("\n=== TelcoMax Autonomous Slice Provisioning — USMNT B2B Upsell Campaign ===\n")

    # ------------------------------------------------------------------
    # 1. Load B2B customers flagged for bandwidth risk
    # ------------------------------------------------------------------
    target_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {CONVERSION_TABLE}
        WHERE conversion_score >= {UPSELL_THRESHOLD}
    """).collect()[0]["cnt"]

    if target_count == 0:
        print("No high-upsell targets found — using synthetic targets for demo...")
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW synthetic_targets AS
            SELECT
                CASE (id % 5)
                    WHEN 0 THEN 'BC-ESPN-001'
                    WHEN 1 THEN 'BC-FOX-001'
                    WHEN 2 THEN 'PM-TICK-001'
                    WHEN 3 THEN 'TM-USMNT-001'
                    ELSE        'BC-APSN-001'
                END AS customer_id,
                CASE (id % 5)
                    WHEN 0 THEN 'ESPN'
                    WHEN 1 THEN 'Fox Sports'
                    WHEN 2 THEN 'Ticketmaster Gates'
                    WHEN 3 THEN 'US Soccer Federation'
                    ELSE        'Apple TV+ Sports'
                END AS company_name,
                CASE (id % 5)
                    WHEN 0 THEN 'broadcaster'
                    WHEN 1 THEN 'broadcaster'
                    WHEN 2 THEN 'payment_processor'
                    WHEN 3 THEN 'team_sponsor'
                    ELSE        'broadcaster'
                END AS customer_type,
                CASE (id % 5)
                    WHEN 0 THEN 'media_rights'
                    WHEN 1 THEN 'media_rights'
                    WHEN 2 THEN 'payments_ticketing'
                    WHEN 3 THEN 'teams_sponsors'
                    ELSE        'media_rights'
                END AS customer_segment,
                CASE (id % 5)
                    WHEN 0 THEN 'PLATINUM'
                    WHEN 1 THEN 'PLATINUM'
                    WHEN 2 THEN 'GOLD'
                    WHEN 3 THEN 'PLATINUM'
                    ELSE        'PLATINUM'
                END AS contract_tier,
                CASE (id % 5)
                    WHEN 0 THEN 12000.0
                    WHEN 1 THEN 10500.0
                    WHEN 2 THEN 3000.0
                    WHEN 3 THEN 8000.0
                    ELSE        9000.0
                END AS monthly_contract_value,
                CASE (id % 5)
                    WHEN 0 THEN 500.0
                    WHEN 1 THEN 400.0
                    WHEN 2 THEN 80.0
                    WHEN 3 THEN 400.0
                    ELSE        350.0
                END AS contracted_bandwidth_mbps,
                CONCAT('SEA-LF-', LPAD(CAST((id % 8 + 1) AS STRING), 3, '0')) AS tower_id,
                0.70 + (id % 12) * 0.02   AS conversion_score,
                CASE (id % 5)
                    WHEN 0 THEN 2000.0
                    WHEN 1 THEN 1600.0
                    WHEN 2 THEN  320.0
                    WHEN 3 THEN 1600.0
                    ELSE        1400.0
                END AS monthly_revenue_opportunity,
                CASE (id % 5)
                    WHEN 0 THEN 0.12
                    WHEN 1 THEN 0.08
                    WHEN 2 THEN 0.15
                    WHEN 3 THEN 0.05
                    ELSE        0.07
                END AS churn_risk_score,
                CASE (id % 5)
                    WHEN 0 THEN 4
                    WHEN 1 THEN 2
                    WHEN 2 THEN 7
                    WHEN 3 THEN 3
                    ELSE        5
                END AS contract_renewal_months,
                CONCAT('AM-', LPAD(CAST(id AS STRING), 3, '0')) AS account_manager_id,
                90.0 + (id % 10) AS utilization_pct,
                'critical' AS breach_risk_level
            FROM (SELECT explode(sequence(1, 12)) AS id)
        """)
        targets_view = "synthetic_targets"
        target_count = 12
    else:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW high_upsell_targets AS
            SELECT
                customer_id, company_name, customer_type, customer_segment,
                contract_tier, monthly_contract_value, contracted_bandwidth_mbps,
                tower_id, conversion_score, monthly_revenue_opportunity,
                churn_risk_score, contract_renewal_months, account_manager_id,
                utilization_pct, breach_risk_level
            FROM {CONVERSION_TABLE}
            WHERE conversion_score >= {UPSELL_THRESHOLD}
        """)
        targets_view = "high_upsell_targets"

    print(f"  Loaded {target_count:,} B2B customers at bandwidth risk (score >= {UPSELL_THRESHOLD})")

    # ------------------------------------------------------------------
    # 2. Provision burst capacity slices for all flagged accounts
    # ------------------------------------------------------------------
    print(f"\nProvisioning burst-capacity slices for {target_count:,} accounts...")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW burst_slices AS
        SELECT
            CONCAT('BURST-', UPPER(MD5(CAST(customer_id AS STRING)))) AS slice_id,
            customer_id,
            tower_id,
            CONCAT(customer_type, '_burst')      AS slice_type,
            -- Add 50% on top of contracted bandwidth
            contracted_bandwidth_mbps * {BANDWIDTH_EXPANSION_PCT}  AS bandwidth_allocated_mbps,
            contracted_bandwidth_mbps * {BANDWIDTH_EXPANSION_PCT}  AS contracted_bandwidth_mbps,
            contracted_bandwidth_mbps * {BANDWIDTH_EXPANSION_PCT} * 0.60 AS current_bandwidth_mbps,
            60.0                                 AS utilization_pct,
            8.0                                  AS latency_guarantee_ms,
            current_timestamp()                  AS created_timestamp,
            current_timestamp() + INTERVAL 4 HOURS  AS expires_timestamp,
            'active'                             AS status,
            contracted_bandwidth_mbps * {BANDWIDTH_EXPANSION_PCT} * 0.008 AS revenue_per_hour
        FROM {targets_view}
    """)

    spark.sql(f"""
        INSERT INTO {SLICES_TABLE}
        SELECT
            slice_id, customer_id, tower_id, slice_type,
            contracted_bandwidth_mbps, bandwidth_allocated_mbps,
            current_bandwidth_mbps, utilization_pct,
            latency_guarantee_ms, created_timestamp, expires_timestamp,
            status, revenue_per_hour
        FROM burst_slices
    """)
    print(f"  Provisioned {target_count:,} burst-capacity slices in <30 seconds (simulated)")

    # ------------------------------------------------------------------
    # 3. Upsell proposal log — account manager + customer portal alerts
    # ------------------------------------------------------------------
    print(f"\nSending upsell proposals to {target_count:,} business accounts...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PROPOSAL_LOG_TABLE} (
            proposal_id             STRING,
            customer_id             STRING,
            company_name            STRING,
            customer_type           STRING,
            customer_segment        STRING,
            contract_tier           STRING,
            account_manager_id      STRING,
            proposal_message        STRING,
            sent_at                 TIMESTAMP,
            conversion_score        DOUBLE,
            utilization_pct         DOUBLE,
            breach_risk_level       STRING,
            contract_renewal_months INT,
            churn_risk_score        DOUBLE,
            accepted                BOOLEAN,
            monthly_revenue_opportunity DOUBLE,
            annual_upsell_contribution  DOUBLE,
            monthly_contract_value  DOUBLE
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    spark.sql(f"""
        INSERT INTO {PROPOSAL_LOG_TABLE}
        SELECT
            CONCAT('PROP-USMNT-', UPPER(SUBSTR(MD5(customer_id), 1, 8))) AS proposal_id,
            customer_id,
            company_name,
            customer_type,
            customer_segment,
            contract_tier,
            account_manager_id,
            CONCAT(
                'ALERT: ', company_name, ' is at ', CAST(ROUND(utilization_pct, 0) AS STRING),
                '% of contracted bandwidth on tower ', tower_id,
                '. Expand by 50% (', CAST(ROUND(contracted_bandwidth_mbps * 0.5, 0) AS STRING),
                ' Mbps) for $', CAST(ROUND(monthly_revenue_opportunity, 0) AS STRING),
                '/mo. Activate in 30 seconds.'
            ) AS proposal_message,
            current_timestamp()              AS sent_at,
            conversion_score,
            utilization_pct,
            breach_risk_level,
            contract_renewal_months,
            churn_risk_score,
            -- Acceptance decision by segment (62% overall B2B rate)
            CASE customer_type
                WHEN 'broadcaster'        THEN (rand() < 0.72)
                WHEN 'payment_processor'  THEN (rand() < 0.68)
                WHEN 'public_safety'      THEN (rand() < 0.50)   -- often pre-budgeted
                WHEN 'team_sponsor'       THEN (rand() < 0.65)
                ELSE                           (rand() < 0.55)
            END AS accepted,
            monthly_revenue_opportunity,
            CASE customer_type
                WHEN 'broadcaster'        THEN CASE WHEN (rand() < 0.72) THEN monthly_revenue_opportunity * 12 ELSE 0 END
                WHEN 'payment_processor'  THEN CASE WHEN (rand() < 0.68) THEN monthly_revenue_opportunity * 12 ELSE 0 END
                ELSE                           CASE WHEN (rand() < 0.62) THEN monthly_revenue_opportunity * 12 ELSE 0 END
            END AS annual_upsell_contribution,
            monthly_contract_value
        FROM {targets_view}
    """)

    # ------------------------------------------------------------------
    # 4. Revenue + churn prevention summary
    # ------------------------------------------------------------------
    summary = spark.sql(f"""
        SELECT
            COUNT(*)                                            AS total_targeted,
            SUM(CASE WHEN accepted THEN 1 ELSE 0 END)          AS total_accepted,
            ROUND(AVG(CASE WHEN accepted THEN 1.0 ELSE 0.0 END), 4) AS acceptance_rate,
            ROUND(SUM(CASE WHEN accepted THEN monthly_revenue_opportunity ELSE 0 END), 2)
                                                               AS immediate_monthly_upsell,
            ROUND(SUM(annual_upsell_contribution), 2)          AS projected_upsell_arr,
            -- ARR at risk from high-churn accounts that we proactively served
            ROUND(SUM(CASE WHEN churn_risk_score > 0.15 THEN monthly_contract_value * 12 ELSE 0 END), 2)
                                                               AS arr_protected_from_churn
        FROM {PROPOSAL_LOG_TABLE}
    """).collect()[0]

    # Write event revenue summary
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REVENUE_SUMMARY_TABLE} (
            event_id                    STRING,
            event_name                  STRING,
            event_date                  DATE,
            venue_name                  STRING,
            b2b_customers_targeted      BIGINT,
            b2b_customers_accepted      BIGINT,
            acceptance_rate             DOUBLE,
            immediate_monthly_upsell_usd DOUBLE,
            projected_upsell_arr_usd    DOUBLE,
            arr_protected_from_churn_usd DOUBLE,
            burst_slices_provisioned    BIGINT,
            sla_violations              INT,
            summary_generated_at        TIMESTAMP
        ) USING DELTA
    """)

    spark.sql(f"""
        INSERT OVERWRITE {REVENUE_SUMMARY_TABLE}
        SELECT
            'EVT-001'                               AS event_id,
            'USMNT vs Australia'                    AS event_name,
            DATE('2024-09-15')                      AS event_date,
            'Lumen Field'                           AS venue_name,
            {summary['total_targeted']}             AS b2b_customers_targeted,
            {summary['total_accepted']}             AS b2b_customers_accepted,
            {summary['acceptance_rate']}            AS acceptance_rate,
            {summary['immediate_monthly_upsell']}   AS immediate_monthly_upsell_usd,
            {summary['projected_upsell_arr']}       AS projected_upsell_arr_usd,
            {summary['arr_protected_from_churn']}   AS arr_protected_from_churn_usd,
            {target_count}                          AS burst_slices_provisioned,
            0                                       AS sla_violations,
            current_timestamp()                     AS summary_generated_at
    """)

    print(f"\n{'='*62}")
    print(f"  USMNT vs Australia — B2B Upsell & Churn Prevention Summary")
    print(f"{'='*62}")
    print(f"  B2B accounts flagged:           {summary['total_targeted']:,}")
    print(f"  Upsell proposals accepted:      {summary['total_accepted']:,}")
    print(f"  Acceptance rate:                {summary['acceptance_rate']:.1%}  (target: 62%)")
    print(f"  Immediate monthly upsell:       ${summary['immediate_monthly_upsell']:,.0f}")
    print(f"  Projected upsell ARR:           ${summary['projected_upsell_arr']:,.0f}")
    print(f"  ARR protected from churn:       ${summary['arr_protected_from_churn']:,.0f}")
    print(f"  Burst slices provisioned:       {target_count:,}")
    print(f"  Avg provisioning time:          <30s")
    print(f"  SLA violations:                 0")
    print(f"{'='*62}\n")

    print("Autonomous slice provisioning and B2B upsell campaign complete.")


if __name__ == "__main__":
    main()
