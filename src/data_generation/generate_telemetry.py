"""
Generate synthetic cell tower telemetry for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_cell_tower_telemetry

Simulates 50 towers over 4 hours around the USMNT vs Australia match at Lumen Field.
8 towers near Lumen Field (SEA-LF-001 through SEA-LF-008) experience the traffic surge.
Time interval: 1 minute (demo scale; 50 towers × 240 intervals = ~12K baseline rows + spikes).
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_cell_tower_telemetry"
SEED = 42


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


# Event timeline
EVENT_START = datetime(2024, 9, 15, 19, 0, 0)
SIM_START = EVENT_START - timedelta(hours=2)   # T-120 min
SIM_END = EVENT_START + timedelta(hours=2, minutes=30)  # T+150 min
INTERVAL_MINUTES = 1

# Tower definitions: 8 near Lumen Field, 42 elsewhere in Seattle metro
LUMEN_TOWERS = [
    {"tower_id": "SEA-LF-001", "latitude": 47.5952, "longitude": -122.3316},
    {"tower_id": "SEA-LF-002", "latitude": 47.5965, "longitude": -122.3330},
    {"tower_id": "SEA-LF-003", "latitude": 47.5940, "longitude": -122.3305},
    {"tower_id": "SEA-LF-004", "latitude": 47.5975, "longitude": -122.3345},
    {"tower_id": "SEA-LF-005", "latitude": 47.5935, "longitude": -122.3350},
    {"tower_id": "SEA-LF-006", "latitude": 47.5960, "longitude": -122.3295},
    {"tower_id": "SEA-LF-007", "latitude": 47.5980, "longitude": -122.3310},
    {"tower_id": "SEA-LF-008", "latitude": 47.5925, "longitude": -122.3330},
]

OTHER_TOWER_COORDS = [
    # Downtown Seattle
    {"tower_id": "SEA-DT-001", "latitude": 47.6062, "longitude": -122.3321},
    {"tower_id": "SEA-DT-002", "latitude": 47.6080, "longitude": -122.3350},
    {"tower_id": "SEA-DT-003", "latitude": 47.6050, "longitude": -122.3300},
    {"tower_id": "SEA-DT-004", "latitude": 47.6100, "longitude": -122.3400},
    {"tower_id": "SEA-DT-005", "latitude": 47.6020, "longitude": -122.3280},
    # Capitol Hill
    {"tower_id": "SEA-CH-001", "latitude": 47.6180, "longitude": -122.3200},
    {"tower_id": "SEA-CH-002", "latitude": 47.6200, "longitude": -122.3220},
    {"tower_id": "SEA-CH-003", "latitude": 47.6155, "longitude": -122.3185},
    # Belltown
    {"tower_id": "SEA-BT-001", "latitude": 47.6135, "longitude": -122.3480},
    {"tower_id": "SEA-BT-002", "latitude": 47.6150, "longitude": -122.3510},
    # South Lake Union
    {"tower_id": "SEA-SL-001", "latitude": 47.6270, "longitude": -122.3390},
    {"tower_id": "SEA-SL-002", "latitude": 47.6250, "longitude": -122.3360},
    # Pioneer Square
    {"tower_id": "SEA-PS-001", "latitude": 47.6005, "longitude": -122.3323},
    {"tower_id": "SEA-PS-002", "latitude": 47.5995, "longitude": -122.3340},
    # International District
    {"tower_id": "SEA-ID-001", "latitude": 47.5980, "longitude": -122.3240},
    {"tower_id": "SEA-ID-002", "latitude": 47.5970, "longitude": -122.3260},
    # First Hill
    {"tower_id": "SEA-FH-001", "latitude": 47.6080, "longitude": -122.3170},
    {"tower_id": "SEA-FH-002", "latitude": 47.6090, "longitude": -122.3190},
    # Fremont
    {"tower_id": "SEA-FR-001", "latitude": 47.6510, "longitude": -122.3500},
    {"tower_id": "SEA-FR-002", "latitude": 47.6490, "longitude": -122.3520},
    # Ballard
    {"tower_id": "SEA-BA-001", "latitude": 47.6680, "longitude": -122.3840},
    {"tower_id": "SEA-BA-002", "latitude": 47.6700, "longitude": -122.3860},
    # U District
    {"tower_id": "SEA-UD-001", "latitude": 47.6585, "longitude": -122.3130},
    {"tower_id": "SEA-UD-002", "latitude": 47.6600, "longitude": -122.3110},
    # Beacon Hill
    {"tower_id": "SEA-BH-001", "latitude": 47.5665, "longitude": -122.3050},
    {"tower_id": "SEA-BH-002", "latitude": 47.5680, "longitude": -122.3070},
    # West Seattle
    {"tower_id": "SEA-WS-001", "latitude": 47.5630, "longitude": -122.3867},
    {"tower_id": "SEA-WS-002", "latitude": 47.5610, "longitude": -122.3900},
    # Rainier Valley
    {"tower_id": "SEA-RV-001", "latitude": 47.5415, "longitude": -122.2843},
    {"tower_id": "SEA-RV-002", "latitude": 47.5430, "longitude": -122.2860},
    # Northgate
    {"tower_id": "SEA-NG-001", "latitude": 47.7065, "longitude": -122.3270},
    {"tower_id": "SEA-NG-002", "latitude": 47.7080, "longitude": -122.3250},
    # Shoreline
    {"tower_id": "SEA-SH-001", "latitude": 47.7560, "longitude": -122.3400},
    # Renton
    {"tower_id": "SEA-RN-001", "latitude": 47.4829, "longitude": -122.2171},
    # Bellevue
    {"tower_id": "SEA-BV-001", "latitude": 47.6101, "longitude": -122.2015},
    {"tower_id": "SEA-BV-002", "latitude": 47.6120, "longitude": -122.2040},
    # Kirkland
    {"tower_id": "SEA-KL-001", "latitude": 47.6815, "longitude": -122.2087},
    # Redmond
    {"tower_id": "SEA-RD-001", "latitude": 47.6740, "longitude": -122.1215},
    # Burien
    {"tower_id": "SEA-BU-001", "latitude": 47.4704, "longitude": -122.3468},
    # SeaTac
    {"tower_id": "SEA-ST-001", "latitude": 47.4502, "longitude": -122.3088},
    # Mercer Island
    {"tower_id": "SEA-MI-001", "latitude": 47.5707, "longitude": -122.2221},
    # Tukwila
    {"tower_id": "SEA-TW-001", "latitude": 47.4685, "longitude": -122.2621},
]

ALL_TOWERS = LUMEN_TOWERS + OTHER_TOWER_COORDS
LUMEN_IDS = {t["tower_id"] for t in LUMEN_TOWERS}


def bandwidth_profile(ts: datetime, is_lumen: bool, rng) -> float:
    """Return realistic bandwidth utilization (0-100%) based on time relative to event."""
    minutes_to_event = (ts - EVENT_START).total_seconds() / 60

    if is_lumen:
        if minutes_to_event < -90:
            base = rng.uniform(18, 28)
        elif minutes_to_event < -60:
            base = rng.uniform(25, 40)
        elif minutes_to_event < -30:
            base = rng.uniform(38, 58)
        elif minutes_to_event < -15:
            base = rng.uniform(58, 72)
        elif minutes_to_event < 0:
            base = rng.uniform(72, 84)   # congestion predicted window
        elif minutes_to_event < 15:
            base = rng.uniform(85, 95)   # kickoff
        elif minutes_to_event < 73:
            base = rng.uniform(80, 92)   # match in progress
        elif minutes_to_event < 76:
            base = rng.uniform(92, 99)   # GOAL scored at T+73
        elif minutes_to_event < 105:
            base = rng.uniform(82, 91)   # post-goal settling
        elif minutes_to_event < 120:
            base = rng.uniform(70, 82)   # final whistle
        else:
            base = rng.uniform(30, 50)   # post-event exit
    else:
        # Other towers: mild background variation
        hour = ts.hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            base = rng.uniform(22, 38)
        elif 10 <= hour <= 16:
            base = rng.uniform(18, 32)
        elif 20 <= hour <= 22:
            base = rng.uniform(15, 28)
        else:
            base = rng.uniform(8, 18)

    noise = rng.normal(0, 2.0)
    return float(np.clip(base + noise, 0, 100))


def generate_telemetry(seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    timestamps = []
    t = SIM_START
    while t <= SIM_END:
        timestamps.append(t)
        t += timedelta(minutes=INTERVAL_MINUTES)

    rows = []
    for tower in ALL_TOWERS:
        tid = tower["tower_id"]
        lat = tower["latitude"]
        lng = tower["longitude"]
        is_lumen = tid in LUMEN_IDS

        for ts in timestamps:
            bw = bandwidth_profile(ts, is_lumen, rng)

            # Derived metrics correlate with bandwidth
            congestion_factor = max(0, (bw - 50) / 50)  # 0 at 50%, 1 at 100%
            latency = rng.uniform(5, 15) + congestion_factor * rng.uniform(50, 150)
            packet_loss = rng.uniform(0, 0.1) + congestion_factor * rng.uniform(0, 8)
            throughput = rng.uniform(200, 400) * (1 - congestion_factor * 0.6)
            error_rate = rng.uniform(0, 0.5) + congestion_factor * rng.uniform(0, 5)
            connections = int(rng.uniform(50, 200) + congestion_factor * rng.uniform(400, 1200))
            signal = rng.uniform(-85, -55)
            temp = rng.uniform(35, 55) + congestion_factor * 15
            power = rng.uniform(800, 1200) + congestion_factor * 400

            rows.append({
                "tower_id": tid,
                "timestamp": ts,
                "latitude": lat,
                "longitude": lng,
                "active_connections": connections,
                "bandwidth_utilization_pct": round(bw, 2),
                "signal_strength_dbm": round(signal, 1),
                "latency_ms": round(latency, 2),
                "packet_loss_pct": round(float(np.clip(packet_loss, 0, 100)), 4),
                "throughput_mbps": round(float(np.clip(throughput, 0, 1000)), 2),
                "error_rate_pct": round(float(np.clip(error_rate, 0, 100)), 4),
                "temperature_celsius": round(temp, 1),
                "power_consumption_watts": round(power, 1),
            })

    return pd.DataFrame(rows)


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print(f"Generating telemetry for {len(ALL_TOWERS)} towers over {int((SIM_END - SIM_START).total_seconds() / 60)} minutes...")
    df_pandas = generate_telemetry()
    total = len(df_pandas)
    print(f"  Generated {total:,} telemetry records")

    lumen_peak = df_pandas[df_pandas["tower_id"].isin(LUMEN_IDS)]["bandwidth_utilization_pct"].max()
    print(f"  Peak Lumen Field bandwidth utilization: {lumen_peak:.1f}%")

    print(f"Writing to {TABLE}...")
    chunk_size = 50_000
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
