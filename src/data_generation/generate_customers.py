"""
Generate synthetic B2B business customer data for TelcoMax demo.
Writes to: cmegdemos_catalog.dynamic_slicing_live_event.bronze_customer_profiles

Represents the five major stadium business customer segments:
  - Broadcasters (uplink-heavy, high contract value)
  - Venue operator (IoT/CCTV/signage)
  - Security / public safety (priority slice, critical reliability)
  - Payment processors / ticketing (low latency, high reliability)
  - Teams, leagues, and sponsors (AR/VR, analytics)
"""

import os
import numpy as np
import pandas as pd
from datetime import date, timedelta

CATALOG = "cmegdemos_catalog"
SCHEMA = "dynamic_slicing_live_event"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_customer_profiles"
SEED = 42


def get_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.serverless(True).getOrCreate()


def generate_customers(seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    today = date.today()

    rows = []

    # ------------------------------------------------------------------
    # 1. Broadcasters & media rights holders (8 accounts)
    # ------------------------------------------------------------------
    broadcasters = [
        ("BC-ESPN-001",    "ESPN",               "broadcaster", "platinum", 12_000, 500, 8.0),
        ("BC-FOX-001",     "Fox Sports",          "broadcaster", "platinum", 10_500, 400, 8.0),
        ("BC-NBC-001",     "NBC Sports",          "broadcaster", "gold",      8_200, 300, 10.0),
        ("BC-PARA-001",    "Paramount+",          "broadcaster", "gold",      7_500, 250, 10.0),
        ("BC-TUDN-001",    "TUDN / Univision",    "broadcaster", "gold",      5_800, 200, 12.0),
        ("BC-APSN-001",    "Apple TV+ Sports",    "broadcaster", "platinum",  9_000, 350, 8.0),
        ("BC-KOMOn001",    "KOMO News (local)",   "broadcaster", "standard",  1_200,  80, 20.0),
        ("BC-KIRO-001",    "KIRO 7 (local)",      "broadcaster", "standard",  1_000,  60, 20.0),
    ]
    for cid, name, ctype, tier, mrv, bw, lat in broadcasters:
        rows.append(_make_row(rng, today, cid, name, ctype, tier, mrv, bw, lat))

    # ------------------------------------------------------------------
    # 2. Stadium / venue operator (2 accounts)
    # ------------------------------------------------------------------
    venue_ops = [
        ("VN-LF-001",  "Lumen Field Ops",          "venue_operator", "gold",     4_500, 300, 15.0),
        ("VN-AEG-001", "AEG Concessions Ops",      "venue_operator", "standard", 1_800, 120, 25.0),
    ]
    for cid, name, ctype, tier, mrv, bw, lat in venue_ops:
        rows.append(_make_row(rng, today, cid, name, ctype, tier, mrv, bw, lat))

    # ------------------------------------------------------------------
    # 3. Security & public safety (4 accounts)
    # ------------------------------------------------------------------
    security = [
        ("SC-SPD-001",  "Seattle Police Dept",     "public_safety", "platinum", 3_200, 150, 5.0),
        ("SC-APG-001",  "Allied Universal Sec",    "public_safety", "gold",     2_100, 100, 8.0),
        ("SC-EMT-001",  "Seattle Fire / EMS",      "public_safety", "platinum", 2_800, 120, 5.0),
        ("SC-FBIf001",  "DHS / Federal Security",  "public_safety", "platinum", 4_500, 200, 5.0),
    ]
    for cid, name, ctype, tier, mrv, bw, lat in security:
        rows.append(_make_row(rng, today, cid, name, ctype, tier, mrv, bw, lat))

    # ------------------------------------------------------------------
    # 4. Ticketing & payments (15 accounts)
    # ------------------------------------------------------------------
    payment_processors = [
        ("PM-TICK-001", "Ticketmaster Gates",      "payment_processor", "gold",     3_000, 80,  5.0),
        ("PM-SQR-001",  "Square POS (merch)",      "payment_processor", "gold",     2_200, 60,  5.0),
        ("PM-SQR-002",  "Square POS (food N)",     "payment_processor", "standard",   800, 30,  8.0),
        ("PM-SQR-003",  "Square POS (food S)",     "payment_processor", "standard",   800, 30,  8.0),
        ("PM-SQR-004",  "Square POS (food E)",     "payment_processor", "standard",   800, 30,  8.0),
        ("PM-SQR-005",  "Square POS (food W)",     "payment_processor", "standard",   800, 30,  8.0),
        ("PM-CLV-001",  "Clover POS (VIP club)",   "payment_processor", "gold",     1_400, 40,  5.0),
        ("PM-STR-001",  "Stripe Mobile Pay",       "payment_processor", "gold",     1_600, 50,  5.0),
        ("PM-AXS-001",  "AXS Mobile Ticketing",    "payment_processor", "gold",     2_500, 70,  8.0),
        ("PM-VND-001",  "Aramark Kiosks N",        "payment_processor", "standard",   600, 20, 10.0),
        ("PM-VND-002",  "Aramark Kiosks S",        "payment_processor", "standard",   600, 20, 10.0),
        ("PM-VND-003",  "Aramark Kiosks E",        "payment_processor", "standard",   600, 20, 10.0),
        ("PM-VND-004",  "Aramark Kiosks W",        "payment_processor", "standard",   600, 20, 10.0),
        ("PM-ATM-001",  "Cardtronics ATMs",        "payment_processor", "standard",   400, 15, 15.0),
        ("PM-PRKG-001", "SP+ Parking Pay",         "payment_processor", "standard",   300, 10, 15.0),
    ]
    for cid, name, ctype, tier, mrv, bw, lat in payment_processors:
        rows.append(_make_row(rng, today, cid, name, ctype, tier, mrv, bw, lat))

    # ------------------------------------------------------------------
    # 5. Teams, leagues, and sponsors (6 accounts)
    # ------------------------------------------------------------------
    teams = [
        ("TM-USMNT-001", "US Soccer Federation",   "team_sponsor", "platinum", 8_000, 400,  8.0),
        ("TM-AUSTR-001", "Football Australia",      "team_sponsor", "gold",     3_500, 150, 10.0),
        ("TM-FIFA-001",  "FIFA / Concacaf Ops",     "team_sponsor", "platinum", 6_000, 250,  8.0),
        ("TM-NIKE-001",  "Nike Sponsorship Ops",    "team_sponsor", "gold",     2_800, 100, 12.0),
        ("TM-BFLY-001",  "Butterfly AR Fan App",    "team_sponsor", "gold",     3_200, 200, 10.0),
        ("TM-STATS-001", "Sportradar Analytics",    "team_sponsor", "gold",     2_400, 120, 10.0),
    ]
    for cid, name, ctype, tier, mrv, bw, lat in teams:
        rows.append(_make_row(rng, today, cid, name, ctype, tier, mrv, bw, lat))

    df = pd.DataFrame(rows)
    return df


def _make_row(rng, today, customer_id, company_name, customer_type, contract_tier,
              monthly_contract_value, contracted_bandwidth_mbps, contracted_latency_ms):
    sla_map = {"standard": "standard", "gold": "gold", "platinum": "platinum"}

    # Churn risk: higher for standard tier accounts with older contracts
    base_churn = {"platinum": 0.04, "gold": 0.10, "standard": 0.22}
    churn_risk = float(np.clip(rng.normal(base_churn[contract_tier], 0.06), 0.01, 0.95))

    # Contract start (1–5 years ago)
    contract_start = today - timedelta(days=int(rng.uniform(365, 1825)))

    # Renewal in 1–18 months (shorter = higher upsell urgency)
    renewal_months = int(rng.uniform(1, 19))

    # Support tickets in last 30 days
    support_tickets = int(rng.choice([0, 0, 0, 1, 1, 2, 3], p=[0.4, 0.2, 0.15, 0.1, 0.08, 0.05, 0.02]))

    # Peak historical utilization (% of contracted bandwidth)
    # — broadcasters and payment processors tend to run close to limit during events
    if customer_type in ("broadcaster", "payment_processor"):
        peak_util = float(np.clip(rng.normal(0.80, 0.10), 0.50, 0.99))
    else:
        peak_util = float(np.clip(rng.normal(0.65, 0.12), 0.30, 0.95))

    return {
        "customer_id": customer_id,
        "company_name": company_name,
        "customer_type": customer_type,
        "contract_tier": contract_tier,
        "monthly_contract_value": monthly_contract_value,
        "contracted_bandwidth_mbps": contracted_bandwidth_mbps,
        "contracted_latency_ms": contracted_latency_ms,
        "sla_tier": sla_map[contract_tier],
        "churn_risk_score": round(churn_risk, 4),
        "contract_start_date": contract_start.isoformat(),
        "contract_renewal_months": renewal_months,
        "support_tickets_30d": support_tickets,
        "peak_event_utilization_pct": round(peak_util, 4),
        "account_manager_id": f"AM-{int(rng.integers(100, 999))}",
    }


def main():
    spark = get_spark()
    print(f"Spark version: {spark.version}")

    print("Generating B2B business customer profiles...")
    df_pandas = generate_customers()
    print(f"  Generated {len(df_pandas):,} business customers")
    print(f"  Type distribution:\n{df_pandas['customer_type'].value_counts()}")
    print(f"  Contract tier distribution:\n{df_pandas['contract_tier'].value_counts()}")
    print(f"  Total contracted bandwidth: {df_pandas['contracted_bandwidth_mbps'].sum():,} Mbps")
    print(f"  Total monthly contract value: ${df_pandas['monthly_contract_value'].sum():,}")

    print(f"\nWriting to {TABLE}...")
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    count = spark.table(TABLE).count()
    print(f"Verified: {count:,} rows in {TABLE}")


if __name__ == "__main__":
    main()
