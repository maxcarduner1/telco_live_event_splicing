"""
Generate synthetic customer profile data for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_customer_profiles
Target: 500K customers with realistic distributions
"""

import os
import numpy as np
import pandas as pd
from datetime import date, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_customer_profiles"
NUM_CUSTOMERS = 500_000
SEED = 42


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def generate_customers(n: int = NUM_CUSTOMERS, seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # Customer IDs
    customer_ids = [f"CUST-{str(i).zfill(7)}" for i in range(1, n + 1)]

    # Subscription tiers: 60% basic, 30% premium, 10% enterprise
    tiers = rng.choice(["basic", "premium", "enterprise"], size=n, p=[0.60, 0.30, 0.10])

    # Monthly revenue by tier
    revenue_map = {"basic": (15, 35), "premium": (60, 120), "enterprise": (200, 500)}
    monthly_revenue = np.array([
        rng.uniform(*revenue_map[t]) for t in tiers
    ])

    # Account start dates (1-8 years ago)
    today = date.today()
    start_dates = [
        (today - timedelta(days=int(rng.uniform(30, 2920)))).isoformat()
        for _ in range(n)
    ]

    # Ages: 18-75, skewed toward 25-45
    ages = rng.integers(18, 76, size=n)

    # Seattle-area ZIP codes (focus for Lumen Field event)
    seattle_zips = ["98101", "98102", "98103", "98104", "98105", "98106",
                    "98107", "98108", "98109", "98112", "98115", "98116",
                    "98117", "98118", "98119", "98121", "98122", "98125",
                    "98126", "98133", "98136", "98144", "98146", "98155",
                    "98177", "98178", "98188", "98195", "98199"]
    other_zips = [f"9{rng.integers(8000, 8999)}" for _ in range(100)]
    all_zips = seattle_zips + other_zips
    location_zips = rng.choice(all_zips, size=n)

    # Device types
    devices = rng.choice(
        ["iPhone", "Samsung Galaxy", "Google Pixel", "OnePlus", "Motorola", "LG"],
        size=n,
        p=[0.35, 0.30, 0.15, 0.08, 0.07, 0.05],
    )

    # Social influence scores (most 0, some up to 1M, 47 with 10K+)
    social_scores = np.zeros(n, dtype=int)
    # ~2% have some following
    has_social = rng.choice(n, size=int(n * 0.02), replace=False)
    social_scores[has_social] = rng.integers(100, 5000, size=len(has_social))
    # 47 influencers with 10K+
    influencer_idx = rng.choice(has_social, size=47, replace=False)
    social_scores[influencer_idx] = rng.integers(10_000, 500_000, size=47)

    # Data usage (GB/month)
    data_usage = np.where(
        tiers == "enterprise",
        rng.uniform(50, 200, size=n),
        np.where(
            tiers == "premium",
            rng.uniform(15, 80, size=n),
            rng.uniform(2, 25, size=n),
        ),
    )

    # SLA tiers
    sla_map = {"basic": "standard", "premium": "gold", "enterprise": "platinum"}
    sla_tiers = [sla_map[t] for t in tiers]

    # Churn risk (higher for basic, lower for enterprise)
    churn_base = {"basic": 0.3, "premium": 0.12, "enterprise": 0.04}
    churn_risk = np.clip(
        np.array([rng.normal(churn_base[t], 0.1) for t in tiers]), 0.01, 0.99
    )

    # Last upgrade date
    last_upgrade = [
        (today - timedelta(days=int(rng.uniform(0, 730)))).isoformat()
        if rng.random() > 0.3
        else None
        for _ in range(n)
    ]

    # Support tickets (last 30 days)
    support_tickets = rng.choice([0, 0, 0, 0, 1, 1, 2, 3, 4, 5], size=n)

    df = pd.DataFrame({
        "customer_id": customer_ids,
        "subscription_tier": tiers,
        "monthly_revenue": monthly_revenue.round(2),
        "account_start_date": pd.to_datetime(start_dates).date,
        "age": ages,
        "location_zip": location_zips,
        "device_type": devices,
        "social_influence_score": social_scores,
        "data_usage_gb_monthly": data_usage.round(2),
        "sla_tier": sla_tiers,
        "churn_risk_score": churn_risk.round(4),
        "last_upgrade_date": pd.to_datetime(last_upgrade).date,
        "support_tickets_30d": support_tickets,
    })

    return df


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print(f"Generating {NUM_CUSTOMERS:,} customer profiles...")
    df_pandas = generate_customers()
    print(f"  Tier distribution:\n{df_pandas['subscription_tier'].value_counts()}")
    influencers = (df_pandas["social_influence_score"] >= 10_000).sum()
    print(f"  Influencers (10K+ followers): {influencers}")

    # Write in chunks for large datasets
    print(f"Writing to {TABLE}...")
    chunk_size = 100_000
    for i in range(0, len(df_pandas), chunk_size):
        chunk = df_pandas.iloc[i:i + chunk_size]
        df_spark = spark.createDataFrame(chunk)
        mode = "overwrite" if i == 0 else "append"
        opts = {"overwriteSchema": "true"} if i == 0 else {}
        df_spark.write.mode(mode).options(**opts).saveAsTable(TABLE)
        print(f"  Written {min(i + chunk_size, len(df_pandas)):,} / {len(df_pandas):,} rows")

    count = spark.table(TABLE).count()
    print(f"Verified: {count:,} rows in {TABLE}")


if __name__ == "__main__":
    main()
