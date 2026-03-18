"""
Generate synthetic event calendar data for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_event_calendar
"""

import os
import pandas as pd
from datetime import datetime, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_event_calendar"


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def generate_events():
    events = [
        {
            "event_id": "EVT-001",
            "event_name": "USMNT vs Australia",
            "venue_name": "Lumen Field",
            "venue_latitude": 47.5952,
            "venue_longitude": -122.3316,
            "event_start_time": datetime(2024, 9, 15, 19, 0, 0),
            "event_end_time": datetime(2024, 9, 15, 21, 30, 0),
            "expected_attendance": 70000,
            "event_type": "sports",
            "traffic_multiplier": 8.47,
        },
        {
            "event_id": "EVT-002",
            "event_name": "Taylor Swift Eras Tour",
            "venue_name": "Climate Pledge Arena",
            "venue_latitude": 47.6220,
            "venue_longitude": -122.3544,
            "event_start_time": datetime(2024, 9, 20, 20, 0, 0),
            "event_end_time": datetime(2024, 9, 20, 23, 30, 0),
            "expected_attendance": 18000,
            "event_type": "concert",
            "traffic_multiplier": 5.2,
        },
        {
            "event_id": "EVT-003",
            "event_name": "Seattle Seahawks vs Dallas Cowboys",
            "venue_name": "Lumen Field",
            "venue_latitude": 47.5952,
            "venue_longitude": -122.3316,
            "event_start_time": datetime(2024, 10, 6, 13, 5, 0),
            "event_end_time": datetime(2024, 10, 6, 16, 30, 0),
            "expected_attendance": 68000,
            "event_type": "sports",
            "traffic_multiplier": 6.8,
        },
        {
            "event_id": "EVT-004",
            "event_name": "AWS re:Invent Preview Summit",
            "venue_name": "Washington State Convention Center",
            "venue_latitude": 47.6114,
            "venue_longitude": -122.3318,
            "event_start_time": datetime(2024, 10, 15, 9, 0, 0),
            "event_end_time": datetime(2024, 10, 15, 18, 0, 0),
            "expected_attendance": 12000,
            "event_type": "conference",
            "traffic_multiplier": 3.1,
        },
        {
            "event_id": "EVT-005",
            "event_name": "Seattle Mariners Playoff Game 1",
            "venue_name": "T-Mobile Park",
            "venue_latitude": 47.5914,
            "venue_longitude": -122.3325,
            "event_start_time": datetime(2024, 10, 8, 17, 7, 0),
            "event_end_time": datetime(2024, 10, 8, 20, 30, 0),
            "expected_attendance": 47929,
            "event_type": "sports",
            "traffic_multiplier": 4.9,
        },
    ]

    return pd.DataFrame(events)


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print("Generating event calendar data...")
    df_pandas = generate_events()
    print(f"  Generated {len(df_pandas)} events")

    df_spark = spark.createDataFrame(df_pandas)
    print(f"Writing to {TABLE}...")
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    count = spark.table(TABLE).count()
    print(f"Verified: {count} rows in {TABLE}")


if __name__ == "__main__":
    main()
