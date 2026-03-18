"""
Autonomous Slice Provisioning & SMS Campaign — TelcoMax Demo
Simulates the USMNT vs Australia match event response using pure Spark SQL.
  1. Queries gold_conversion_opportunities for high-conversion customers
  2. "Provisions" premium slices (writes to bronze_network_slices)
  3. "Sends" SMS upgrade offers (writes to a campaign log table)
  4. Computes and prints revenue summary
"""

import os
from datetime import datetime

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
CONVERSION_TABLE = f"{CATALOG}.{SCHEMA}.gold_conversion_opportunities"
SLICES_TABLE = f"{CATALOG}.{SCHEMA}.bronze_network_slices"
CAMPAIGN_LOG_TABLE = f"{CATALOG}.{SCHEMA}.sms_campaign_log"
REVENUE_SUMMARY_TABLE = f"{CATALOG}.{SCHEMA}.event_revenue_summary"

CONVERSION_THRESHOLD = 0.7
CONVERSION_RATE = 0.47


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
    print("\n=== TelcoMax Autonomous Slice Provisioning — USMNT Match Simulation ===\n")

    # --- 1. Load conversion targets ---
    target_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {CONVERSION_TABLE}
        WHERE conversion_score >= {CONVERSION_THRESHOLD}
    """).collect()[0]["cnt"]

    if target_count == 0:
        print("No high-conversion targets found — using synthetic targets for demo...")
        # Insert synthetic targets into a temp view for this simulation
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW synthetic_targets AS
            SELECT
                CONCAT('CUST-', LPAD(CAST(id AS STRING), 7, '0'))   AS customer_id,
                CASE (id % 5)
                    WHEN 0 THEN 'high_value_influencer'
                    WHEN 1 THEN 'high_value'
                    WHEN 2 THEN 'premium'
                    ELSE 'standard'
                END AS customer_segment,
                CASE (id % 2) WHEN 0 THEN 'BASIC' ELSE 'PREMIUM' END AS subscription_tier,
                CONCAT('SEA-LF-', LPAD(CAST((id % 8 + 1) AS STRING), 3, '0')) AS tower_id,
                0.75 + (id % 20) * 0.01   AS conversion_score,
                CASE (id % 2) WHEN 0 THEN 25.0 ELSE 50.0 END AS monthly_revenue_opportunity,
                CAST(id % 100000 AS BIGINT) AS social_influence_score
            FROM (SELECT explode(sequence(1, 3200)) AS id)
        """)
        targets_view = "synthetic_targets"
        target_count = 3200
    else:
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW high_conv_targets AS
            SELECT
                customer_id, customer_segment, subscription_tier,
                tower_id, conversion_score, monthly_revenue_opportunity,
                social_influence_score
            FROM {CONVERSION_TABLE}
            WHERE conversion_score >= {CONVERSION_THRESHOLD}
        """)
        targets_view = "high_conv_targets"

    print(f"  Loaded {target_count:,} high-conversion targets (score >= {CONVERSION_THRESHOLD})")

    # --- 2. Provision premium slices ---
    print(f"\nProvisioning {target_count:,} premium slices...")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW new_slices AS
        SELECT
            CONCAT('AUTO-', UPPER(MD5(CAST(customer_id AS STRING)))) AS slice_id,
            customer_id,
            tower_id,
            'premium_streaming'        AS slice_type,
            50.0 + (conversion_score * 100.0)  AS bandwidth_allocated_mbps,
            8.0                        AS latency_guarantee_ms,
            current_timestamp()        AS created_timestamp,
            current_timestamp() + INTERVAL 4 HOURS  AS expires_timestamp,
            'active'                   AS status,
            3.75                       AS revenue_per_hour
        FROM {targets_view}
    """)

    spark.sql(f"""
        INSERT INTO {SLICES_TABLE}
        SELECT
            slice_id, customer_id, tower_id, slice_type,
            bandwidth_allocated_mbps, latency_guarantee_ms,
            created_timestamp, expires_timestamp, status, revenue_per_hour
        FROM new_slices
    """)
    print(f"  Provisioned {target_count:,} premium slices in <30 seconds (simulated)")

    # --- 3. SMS campaign log ---
    print(f"\nSending SMS upgrade offers to {target_count:,} customers...")

    # Create campaign log table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CAMPAIGN_LOG_TABLE} (
            campaign_id STRING,
            customer_id STRING,
            customer_segment STRING,
            subscription_tier STRING,
            offer_message STRING,
            sent_at TIMESTAMP,
            conversion_score DOUBLE,
            converted BOOLEAN,
            monthly_revenue_opportunity DOUBLE,
            annual_revenue_contribution DOUBLE,
            social_influence_score BIGINT
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    spark.sql(f"""
        INSERT INTO {CAMPAIGN_LOG_TABLE}
        SELECT
            CONCAT('CAMP-USMNT-', UPPER(SUBSTR(MD5(customer_id), 1, 8))) AS campaign_id,
            customer_id,
            customer_segment,
            subscription_tier,
            'TelcoMax: Guarantee your live stream at Lumen Field! Premium 5G $15/mo - reply YES to activate.' AS offer_message,
            current_timestamp()      AS sent_at,
            conversion_score,
            -- Conversion decision by segment (simulate 47% overall rate)
            CASE customer_segment
                WHEN 'high_value_influencer' THEN (rand() < 0.73)
                WHEN 'high_value'            THEN (rand() < 0.58)
                WHEN 'influencer'            THEN (rand() < 0.71)
                WHEN 'premium'               THEN (rand() < 0.42)
                ELSE                              (rand() < 0.31)
            END AS converted,
            monthly_revenue_opportunity,
            CASE customer_segment
                WHEN 'high_value_influencer' THEN CASE WHEN (rand() < 0.73) THEN monthly_revenue_opportunity * 12 ELSE 0 END
                WHEN 'high_value'            THEN CASE WHEN (rand() < 0.58) THEN monthly_revenue_opportunity * 12 ELSE 0 END
                ELSE                              CASE WHEN (rand() < 0.47) THEN monthly_revenue_opportunity * 12 ELSE 0 END
            END AS annual_revenue_contribution,
            social_influence_score
        FROM {targets_view}
    """)

    # --- 4. Revenue summary ---
    summary = spark.sql(f"""
        SELECT
            COUNT(*) AS total_targeted,
            SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS total_converted,
            ROUND(AVG(CASE WHEN converted THEN 1.0 ELSE 0.0 END), 4) AS conversion_rate,
            ROUND(SUM(CASE WHEN converted THEN monthly_revenue_opportunity ELSE 0 END), 2) AS immediate_revenue,
            ROUND(SUM(annual_revenue_contribution), 2) AS projected_arr
        FROM {CAMPAIGN_LOG_TABLE}
    """).collect()[0]

    # Write event revenue summary
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REVENUE_SUMMARY_TABLE} (
            event_id STRING,
            event_name STRING,
            event_date DATE,
            venue_name STRING,
            customers_targeted BIGINT,
            customers_converted BIGINT,
            conversion_rate DOUBLE,
            immediate_revenue_usd DOUBLE,
            projected_arr_usd DOUBLE,
            slices_provisioned BIGINT,
            sla_violations INT,
            customer_sat_improvement_pct DOUBLE,
            summary_generated_at TIMESTAMP
        ) USING DELTA
    """)

    spark.sql(f"""
        INSERT OVERWRITE {REVENUE_SUMMARY_TABLE}
        SELECT
            'EVT-001'                       AS event_id,
            'USMNT vs Australia'            AS event_name,
            DATE('2024-09-15')              AS event_date,
            'Lumen Field'                   AS venue_name,
            {summary['total_targeted']}     AS customers_targeted,
            {summary['total_converted']}    AS customers_converted,
            {summary['conversion_rate']}    AS conversion_rate,
            {summary['immediate_revenue']}  AS immediate_revenue_usd,
            {summary['projected_arr']}      AS projected_arr_usd,
            {target_count}                  AS slices_provisioned,
            0                               AS sla_violations,
            23.0                            AS customer_sat_improvement_pct,
            current_timestamp()             AS summary_generated_at
    """)

    print(f"\n{'='*55}")
    print(f"  USMNT vs Australia — Revenue Generation Summary")
    print(f"{'='*55}")
    print(f"  Customers targeted:         {summary['total_targeted']:,}")
    print(f"  Customers converted:        {summary['total_converted']:,}")
    print(f"  Conversion rate:            {summary['conversion_rate']:.1%}  (target: 47%)")
    print(f"  Immediate revenue:          ${summary['immediate_revenue']:,.0f}")
    print(f"  Projected ARR:              ${summary['projected_arr']:,.0f}")
    print(f"  Premium slices provisioned: {target_count:,}")
    print(f"  Avg provisioning time:      <30s")
    print(f"  SLA violations:             0")
    print(f"{'='*55}\n")

    print("Autonomous slice provisioning and SMS campaign complete.")


if __name__ == "__main__":
    main()
