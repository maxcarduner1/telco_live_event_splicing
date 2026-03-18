"""
Generate synthetic network slice data for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_network_slices
Includes pre-event baseline slices + simulated autonomous provisioning surge during USMNT match.
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_network_slices"
SEED = 42


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def generate_slices(seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # Towers near Lumen Field
    lumen_towers = [f"SEA-LF-{str(i).zfill(3)}" for i in range(1, 9)]
    other_towers = [f"SEA-DT-{str(i).zfill(3)}" for i in range(1, 21)] + \
                   [f"SEA-NB-{str(i).zfill(3)}" for i in range(1, 16)]
    all_towers = lumen_towers + other_towers

    slice_types = [
        "premium_streaming", "enterprise_iot", "premium_voip",
        "basic_data", "enterprise_mission_critical"
    ]
    slice_type_probs = [0.35, 0.20, 0.25, 0.15, 0.05]

    rows = []
    slice_counter = 1

    # --- Baseline slices (before event): 5,000 ---
    event_start = datetime(2024, 9, 15, 19, 0, 0)
    for i in range(5000):
        tower = rng.choice(all_towers)
        stype = rng.choice(slice_types, p=slice_type_probs)
        created = event_start - timedelta(hours=float(rng.uniform(0, 72)))
        duration_hours = rng.uniform(1, 48)
        expires = created + timedelta(hours=duration_hours)
        status = "active" if expires > event_start else "terminated"
        bw = rng.uniform(5, 100) if "premium" in stype or "enterprise" in stype else rng.uniform(1, 20)
        revenue = bw * rng.uniform(0.05, 0.15)

        rows.append({
            "slice_id": f"SLC-{str(slice_counter).zfill(8)}",
            "customer_id": f"CUST-{str(rng.integers(1, 500001)).zfill(7)}",
            "tower_id": tower,
            "slice_type": stype,
            "bandwidth_allocated_mbps": round(bw, 2),
            "latency_guarantee_ms": round(rng.uniform(5, 50), 1),
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": status,
            "revenue_per_hour": round(revenue, 4),
        })
        slice_counter += 1

    # --- Autonomous provisioning surge during USMNT match: 3,200 slices ---
    # T-15 min through T+90 min, concentrated on Lumen Field towers
    match_start = event_start
    provision_start = match_start - timedelta(minutes=15)
    for i in range(3200):
        tower = rng.choice(lumen_towers, p=[0.20, 0.18, 0.17, 0.14, 0.12, 0.09, 0.06, 0.04])
        created_offset_min = float(rng.uniform(0, 45))
        created = provision_start + timedelta(minutes=created_offset_min)
        duration_hours = rng.uniform(2, 6)
        expires = created + timedelta(hours=duration_hours)

        rows.append({
            "slice_id": f"SLC-{str(slice_counter).zfill(8)}",
            "customer_id": f"CUST-{str(rng.integers(1, 500001)).zfill(7)}",
            "tower_id": tower,
            "slice_type": "premium_streaming",
            "bandwidth_allocated_mbps": round(rng.uniform(25, 100), 2),
            "latency_guarantee_ms": round(rng.uniform(5, 10), 1),
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": "active",
            "revenue_per_hour": round(rng.uniform(0.20, 0.65), 4),
        })
        slice_counter += 1

    # --- Additional historical slices: 2,000 ---
    for i in range(2000):
        tower = rng.choice(all_towers)
        stype = rng.choice(slice_types, p=slice_type_probs)
        created = event_start - timedelta(hours=float(rng.uniform(72, 720)))
        duration_hours = rng.uniform(1, 24)
        expires = created + timedelta(hours=duration_hours)

        rows.append({
            "slice_id": f"SLC-{str(slice_counter).zfill(8)}",
            "customer_id": f"CUST-{str(rng.integers(1, 500001)).zfill(7)}",
            "tower_id": tower,
            "slice_type": stype,
            "bandwidth_allocated_mbps": round(rng.uniform(5, 80), 2),
            "latency_guarantee_ms": round(rng.uniform(5, 50), 1),
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": "terminated",
            "revenue_per_hour": round(rng.uniform(0.01, 0.50), 4),
        })
        slice_counter += 1

    return pd.DataFrame(rows)


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print("Generating network slice data...")
    df_pandas = generate_slices()
    print(f"  Generated {len(df_pandas):,} slices")
    print(f"  Status distribution:\n{df_pandas['status'].value_counts()}")
    print(f"  Type distribution:\n{df_pandas['slice_type'].value_counts()}")

    print(f"Writing to {TABLE}...")
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    count = spark.table(TABLE).count()
    print(f"Verified: {count:,} rows in {TABLE}")


if __name__ == "__main__":
    main()
