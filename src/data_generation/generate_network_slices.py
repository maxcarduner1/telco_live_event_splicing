"""
Generate synthetic network slice data for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_network_slices

Each row represents one active slice belonging to a stadium B2B customer.
Includes:
  - contracted_bandwidth_mbps: what the customer purchased
  - current_bandwidth_mbps: actual usage at snapshot time (simulated)
  - utilization_pct: current / contracted * 100

During the USMNT match simulation, several customers are pushed toward
or beyond 90% utilization, triggering the upsell opportunity pipeline.
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_network_slices"
SEED = 42

# Customer ID → (contracted_bandwidth_mbps, slice_type, latency_guarantee_ms)
CUSTOMER_SLICE_CONFIG = {
    # Broadcasters
    "BC-ESPN-001":    (500, "broadcast_uplink",      8.0),
    "BC-FOX-001":     (400, "broadcast_uplink",      8.0),
    "BC-NBC-001":     (300, "broadcast_uplink",     10.0),
    "BC-PARA-001":    (250, "broadcast_uplink",     10.0),
    "BC-TUDN-001":    (200, "broadcast_uplink",     12.0),
    "BC-APSN-001":    (350, "broadcast_uplink",      8.0),
    "BC-KOMOn001":    ( 80, "broadcast_uplink",     20.0),
    "BC-KIRO-001":    ( 60, "broadcast_uplink",     20.0),
    # Venue operator
    "VN-LF-001":      (300, "venue_operations",     15.0),
    "VN-AEG-001":     (120, "venue_operations",     25.0),
    # Security
    "SC-SPD-001":     (150, "public_safety",         5.0),
    "SC-APG-001":     (100, "public_safety",         8.0),
    "SC-EMT-001":     (120, "public_safety",         5.0),
    "SC-FBIf001":     (200, "public_safety",         5.0),
    # Payment processors
    "PM-TICK-001":    ( 80, "payment_processing",    5.0),
    "PM-SQR-001":     ( 60, "payment_processing",    5.0),
    "PM-SQR-002":     ( 30, "payment_processing",    8.0),
    "PM-SQR-003":     ( 30, "payment_processing",    8.0),
    "PM-SQR-004":     ( 30, "payment_processing",    8.0),
    "PM-SQR-005":     ( 30, "payment_processing",    8.0),
    "PM-CLV-001":     ( 40, "payment_processing",    5.0),
    "PM-STR-001":     ( 50, "payment_processing",    5.0),
    "PM-AXS-001":     ( 70, "payment_processing",    8.0),
    "PM-VND-001":     ( 20, "payment_processing",   10.0),
    "PM-VND-002":     ( 20, "payment_processing",   10.0),
    "PM-VND-003":     ( 20, "payment_processing",   10.0),
    "PM-VND-004":     ( 20, "payment_processing",   10.0),
    "PM-ATM-001":     ( 15, "payment_processing",   15.0),
    "PM-PRKG-001":    ( 10, "payment_processing",   15.0),
    # Teams / leagues / sponsors
    "TM-USMNT-001":  (400, "team_operations",        8.0),
    "TM-AUSTR-001":  (150, "team_operations",       10.0),
    "TM-FIFA-001":   (250, "team_operations",        8.0),
    "TM-NIKE-001":   (100, "team_operations",       12.0),
    "TM-BFLY-001":   (200, "team_operations",       10.0),
    "TM-STATS-001":  (120, "team_operations",       10.0),
}

# Towers near Lumen Field (primary) and broader Seattle network
LUMEN_TOWERS = [f"SEA-LF-{str(i).zfill(3)}" for i in range(1, 9)]
OTHER_TOWERS  = [f"SEA-DT-{str(i).zfill(3)}" for i in range(1, 21)] + \
                [f"SEA-NB-{str(i).zfill(3)}" for i in range(1, 16)]


def generate_slices(seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    event_start = datetime(2024, 9, 15, 19, 0, 0)  # kickoff
    rows = []
    slice_counter = 1

    # -----------------------------------------------------------------
    # Each B2B customer has one primary event-day slice on a Lumen tower
    # -----------------------------------------------------------------
    tower_cycle = iter(LUMEN_TOWERS * 10)  # cycle through the 8 towers
    for customer_id, (contracted_bw, stype, lat_ms) in CUSTOMER_SLICE_CONFIG.items():
        tower = next(tower_cycle)
        created = event_start - timedelta(hours=float(rng.uniform(1, 4)))
        expires = event_start + timedelta(hours=float(rng.uniform(5, 8)))

        # Simulate utilization snapshot at the start of the match:
        # Broadcasters and payment processors run hot; others more relaxed.
        if stype == "broadcast_uplink":
            util_pct = float(np.clip(rng.normal(0.78, 0.08), 0.45, 0.99))
        elif stype == "payment_processing":
            util_pct = float(np.clip(rng.normal(0.70, 0.10), 0.40, 0.98))
        elif stype == "public_safety":
            util_pct = float(np.clip(rng.normal(0.50, 0.08), 0.30, 0.75))
        else:
            util_pct = float(np.clip(rng.normal(0.60, 0.12), 0.30, 0.90))

        current_bw = round(contracted_bw * util_pct, 2)
        revenue_per_hour = round(contracted_bw * rng.uniform(0.002, 0.008), 4)

        rows.append({
            "slice_id": f"SLC-{str(slice_counter).zfill(8)}",
            "customer_id": customer_id,
            "tower_id": tower,
            "slice_type": stype,
            "contracted_bandwidth_mbps": float(contracted_bw),
            "bandwidth_allocated_mbps": float(contracted_bw),   # kept for pipeline compatibility
            "current_bandwidth_mbps": current_bw,
            "utilization_pct": round(util_pct * 100, 2),
            "latency_guarantee_ms": lat_ms,
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": "active",
            "revenue_per_hour": revenue_per_hour,
        })
        slice_counter += 1

    # -----------------------------------------------------------------
    # Additional baseline slices (non-event operational / historical)
    # -----------------------------------------------------------------
    all_towers = LUMEN_TOWERS + OTHER_TOWERS
    slice_types = list(CUSTOMER_SLICE_CONFIG.values())
    all_customer_ids = list(CUSTOMER_SLICE_CONFIG.keys())

    for _ in range(200):
        cid = rng.choice(all_customer_ids)
        contracted_bw, stype, lat_ms = CUSTOMER_SLICE_CONFIG[cid]
        tower = rng.choice(all_towers)
        created = event_start - timedelta(hours=float(rng.uniform(0, 168)))
        duration = timedelta(hours=float(rng.uniform(1, 48)))
        expires = created + duration
        status = "active" if expires > event_start else "terminated"
        util_pct = float(np.clip(rng.normal(0.55, 0.15), 0.10, 0.95))

        rows.append({
            "slice_id": f"SLC-{str(slice_counter).zfill(8)}",
            "customer_id": cid,
            "tower_id": tower,
            "slice_type": stype,
            "contracted_bandwidth_mbps": float(contracted_bw),
            "bandwidth_allocated_mbps": float(contracted_bw),
            "current_bandwidth_mbps": round(contracted_bw * util_pct, 2),
            "utilization_pct": round(util_pct * 100, 2),
            "latency_guarantee_ms": lat_ms,
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": status,
            "revenue_per_hour": round(contracted_bw * rng.uniform(0.001, 0.005), 4),
        })
        slice_counter += 1

    # -----------------------------------------------------------------
    # Congestion surge: during the 73rd-minute goal, ~12 customers breach
    # their 90% threshold.  These rows represent the peak-utilization
    # snapshot that the pipeline will score as upsell opportunities.
    # -----------------------------------------------------------------
    surge_targets = [
        ("BC-ESPN-001", 0.97),
        ("BC-FOX-001",  0.94),
        ("BC-APSN-001", 0.93),
        ("BC-NBC-001",  0.91),
        ("PM-TICK-001", 0.96),
        ("PM-SQR-001",  0.95),
        ("PM-AXS-001",  0.92),
        ("PM-CLV-001",  0.90),
        ("TM-USMNT-001", 0.94),
        ("TM-BFLY-001",  0.91),
        ("VN-LF-001",    0.89),
        ("TM-FIFA-001",  0.88),
    ]
    goal_time = event_start + timedelta(minutes=73)
    for cid, util_pct in surge_targets:
        contracted_bw, stype, lat_ms = CUSTOMER_SLICE_CONFIG[cid]
        tower = LUMEN_TOWERS[int(rng.integers(0, len(LUMEN_TOWERS)))]
        created = goal_time - timedelta(minutes=float(rng.uniform(1, 5)))
        expires = created + timedelta(hours=4)

        rows.append({
            "slice_id": f"SURGE-{str(slice_counter).zfill(8)}",
            "customer_id": cid,
            "tower_id": tower,
            "slice_type": stype,
            "contracted_bandwidth_mbps": float(contracted_bw),
            "bandwidth_allocated_mbps": float(contracted_bw),
            "current_bandwidth_mbps": round(contracted_bw * util_pct, 2),
            "utilization_pct": round(util_pct * 100, 2),
            "latency_guarantee_ms": lat_ms,
            "created_timestamp": created,
            "expires_timestamp": expires,
            "status": "active",
            "revenue_per_hour": round(contracted_bw * rng.uniform(0.002, 0.008), 4),
        })
        slice_counter += 1

    return pd.DataFrame(rows)


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print("Generating B2B network slice data...")
    df_pandas = generate_slices()
    print(f"  Generated {len(df_pandas):,} slices")
    print(f"  Status distribution:\n{df_pandas['status'].value_counts()}")
    print(f"  Type distribution:\n{df_pandas['slice_type'].value_counts()}")
    surge = df_pandas[df_pandas["utilization_pct"] >= 88]
    print(f"  Slices >= 88% utilization (upsell targets): {len(surge)}")
    print(f"  Customers at risk: {surge['customer_id'].nunique()}")

    print(f"\nWriting to {TABLE}...")
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    count = spark.table(TABLE).count()
    print(f"Verified: {count:,} rows in {TABLE}")


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


if __name__ == "__main__":
    main()
