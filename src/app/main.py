"""
TelcoMax Dynamic 5G Network Slicing - Live Event Visualization (B2B Edition)
FastAPI backend serving data from Databricks SQL Warehouse.

Customer model: B2B stadium business accounts (broadcasters, venue ops,
security, payment processors, teams/sponsors) — not individual consumers.
"""

import os
import math
import random
import hashlib
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from databricks import sql as databricks_sql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG_SCHEMA = "cmegdemos_catalog.dynamic_slicing_live_event"
WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/9cd919d96b11bf1c"

# Tower positions (lat, lon) for Lumen Field area
TOWER_POSITIONS = {
    "SEA-LF-001": (47.5950, -122.3320),
    "SEA-LF-002": (47.5945, -122.3315),
    "SEA-LF-003": (47.5955, -122.3325),
    "SEA-LF-004": (47.5940, -122.3310),
    "SEA-LF-005": (47.5960, -122.3330),
    "SEA-LF-006": (47.5948, -122.3335),
    "SEA-LF-007": (47.5942, -122.3305),
    "SEA-LF-008": (47.5958, -122.3312),
}
STADIUM_CENTER = (47.5952, -122.3316)

# All 35 B2B accounts with their types and assigned towers (for demo fallback)
B2B_ACCOUNTS = [
    ("BC-ESPN-001",    "ESPN",               "broadcaster",       "SEA-LF-001"),
    ("BC-FOX-001",     "Fox Sports",          "broadcaster",       "SEA-LF-001"),
    ("BC-NBC-001",     "NBC Sports",          "broadcaster",       "SEA-LF-002"),
    ("BC-PARA-001",    "Paramount+",          "broadcaster",       "SEA-LF-002"),
    ("BC-TUDN-001",    "TUDN / Univision",    "broadcaster",       "SEA-LF-003"),
    ("BC-APSN-001",    "Apple TV+ Sports",    "broadcaster",       "SEA-LF-003"),
    ("BC-KOMOn001",    "KOMO News",           "broadcaster",       "SEA-LF-004"),
    ("BC-KIRO-001",    "KIRO 7",              "broadcaster",       "SEA-LF-004"),
    ("VN-LF-001",      "Lumen Field Ops",     "venue_operator",    "SEA-LF-005"),
    ("VN-AEG-001",     "AEG Concessions",     "venue_operator",    "SEA-LF-005"),
    ("SC-SPD-001",     "Seattle Police Dept", "public_safety",     "SEA-LF-006"),
    ("SC-APG-001",     "Allied Universal",    "public_safety",     "SEA-LF-006"),
    ("SC-EMT-001",     "Seattle Fire/EMS",    "public_safety",     "SEA-LF-007"),
    ("SC-FBIf001",     "DHS Security",        "public_safety",     "SEA-LF-007"),
    ("PM-TICK-001",    "Ticketmaster Gates",  "payment_processor", "SEA-LF-001"),
    ("PM-SQR-001",     "Square POS (merch)",  "payment_processor", "SEA-LF-002"),
    ("PM-SQR-002",     "Square POS (N)",      "payment_processor", "SEA-LF-003"),
    ("PM-SQR-003",     "Square POS (S)",      "payment_processor", "SEA-LF-004"),
    ("PM-SQR-004",     "Square POS (E)",      "payment_processor", "SEA-LF-005"),
    ("PM-SQR-005",     "Square POS (W)",      "payment_processor", "SEA-LF-006"),
    ("PM-CLV-001",     "Clover POS (VIP)",    "payment_processor", "SEA-LF-007"),
    ("PM-STR-001",     "Stripe Mobile Pay",   "payment_processor", "SEA-LF-008"),
    ("PM-AXS-001",     "AXS Ticketing",       "payment_processor", "SEA-LF-001"),
    ("PM-VND-001",     "Aramark Kiosks N",    "payment_processor", "SEA-LF-002"),
    ("PM-VND-002",     "Aramark Kiosks S",    "payment_processor", "SEA-LF-003"),
    ("PM-VND-003",     "Aramark Kiosks E",    "payment_processor", "SEA-LF-004"),
    ("PM-VND-004",     "Aramark Kiosks W",    "payment_processor", "SEA-LF-005"),
    ("PM-ATM-001",     "Cardtronics ATMs",    "payment_processor", "SEA-LF-006"),
    ("PM-PRKG-001",    "SP+ Parking Pay",     "payment_processor", "SEA-LF-007"),
    ("TM-USMNT-001",   "US Soccer Fed",       "team_sponsor",      "SEA-LF-008"),
    ("TM-AUSTR-001",   "Football Australia",  "team_sponsor",      "SEA-LF-001"),
    ("TM-FIFA-001",    "FIFA / Concacaf",     "team_sponsor",      "SEA-LF-002"),
    ("TM-NIKE-001",    "Nike Sponsor Ops",    "team_sponsor",      "SEA-LF-003"),
    ("TM-BFLY-001",    "Butterfly AR App",    "team_sponsor",      "SEA-LF-004"),
    ("TM-STATS-001",   "Sportradar",          "team_sponsor",      "SEA-LF-005"),
]

# Simulation: 30 steps representing the match timeline
TOTAL_SIMULATION_STEPS = 30


def _get_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "https://fevm-cmegdemos.cloud.databricks.com")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _get_token() -> str:
    """Get auth token - Apps OAuth (DATABRICKS_CLIENT_ID/SECRET), env var, or SDK."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config
        cfg = Config()
        headers = cfg.authenticate()
        if headers and "Authorization" in headers:
            return headers["Authorization"].replace("Bearer ", "")
    except Exception:
        pass
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    raise RuntimeError("No auth token available. Configure DATABRICKS_TOKEN or deploy via Databricks Apps.")


def get_connection():
    return databricks_sql.connect(
        server_hostname=_get_host().replace("https://", ""),
        http_path=WAREHOUSE_HTTP_PATH,
        access_token=_get_token(),
    )


def execute_query(sql: str, params=None):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        execute_query("SELECT 1")
        print("[OK] Databricks SQL connection verified")
    except Exception as e:
        print(f"[WARN] Databricks SQL connection test failed: {e}")
    yield


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(title="TelcoMax Live Event — B2B Edition", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/api/kpis")
def get_kpis():
    """Return all KPI numbers for the top bar (B2B edition)."""
    try:
        r1 = execute_query(f"""
            SELECT COUNT(DISTINCT customer_id) AS cnt
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities
        """)
        accounts_at_risk = r1[0]["cnt"] if r1 else 0

        r2 = execute_query(f"""
            SELECT COUNT(DISTINCT tower_id) AS cnt
            FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
        """)
        active_towers = r2[0]["cnt"] if r2 else 0

        r3 = execute_query(f"""
            SELECT COUNT(DISTINCT tower_id) AS cnt
            FROM {CATALOG_SCHEMA}.gold_congestion_features
            WHERE congestion_predicted_15min = 1
        """)
        towers_congested = r3[0]["cnt"] if r3 else 0

        r4 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.upsell_proposal_log
        """)
        proposals_sent = r4[0]["cnt"] if r4 else 0

        r5 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.upsell_proposal_log
            WHERE accepted = true
        """)
        proposals_accepted = r5[0]["cnt"] if r5 else 0

        r6 = execute_query(f"""
            SELECT projected_upsell_arr_usd
            FROM {CATALOG_SCHEMA}.event_revenue_summary
            ORDER BY summary_generated_at DESC
            LIMIT 1
        """)
        upsell_arr = float(r6[0]["projected_upsell_arr_usd"]) if r6 else 0

        r7 = execute_query(f"""
            SELECT arr_protected_from_churn_usd
            FROM {CATALOG_SCHEMA}.event_revenue_summary
            ORDER BY summary_generated_at DESC
            LIMIT 1
        """)
        arr_protected = float(r7[0]["arr_protected_from_churn_usd"]) if r7 else 0

        r8 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.bronze_network_slices
            WHERE status = 'active' AND slice_type LIKE '%_burst'
        """)
        burst_slices = r8[0]["cnt"] if r8 else 0

        return {
            "accounts_at_risk": accounts_at_risk,
            "active_towers": active_towers,
            "towers_congested": towers_congested,
            "proposals_sent": proposals_sent,
            "proposals_accepted": proposals_accepted,
            "upsell_arr": upsell_arr,
            "arr_protected": arr_protected,
            "burst_slices": burst_slices,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/towers")
def get_towers():
    """Return tower positions and congestion status."""
    try:
        rows = execute_query(f"""
            SELECT
                t.tower_id,
                t.bandwidth_utilization_pct,
                t.active_connections,
                t.latency_ms,
                cf.avg_congestion_score AS congestion_score,
                cf.congestion_predicted_15min,
                cf.avg_bandwidth_util,
                cf.peak_bandwidth_util,
                cf.bandwidth_trend_15min
            FROM (
                SELECT tower_id,
                       bandwidth_utilization_pct,
                       active_connections,
                       latency_ms,
                       ROW_NUMBER() OVER (PARTITION BY tower_id ORDER BY timestamp DESC) AS rn
                FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
                WHERE tower_id LIKE 'SEA-LF-%'
            ) t
            LEFT JOIN (
                SELECT tower_id,
                       congestion_predicted_15min,
                       avg_bandwidth_util,
                       peak_bandwidth_util,
                       avg_congestion_score,
                       bandwidth_trend_15min,
                       ROW_NUMBER() OVER (PARTITION BY tower_id ORDER BY window_start DESC) AS rn
                FROM {CATALOG_SCHEMA}.gold_congestion_features
                WHERE tower_id LIKE 'SEA-LF-%'
            ) cf ON t.tower_id = cf.tower_id AND cf.rn = 1
            WHERE t.rn = 1
            ORDER BY t.tower_id
        """)

        towers = []
        for row in rows:
            tid = row["tower_id"]
            lat, lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            towers.append({
                "tower_id": tid,
                "latitude": lat,
                "longitude": lon,
                "bandwidth_utilization_pct": _to_float(row.get("bandwidth_utilization_pct")),
                "active_connections": _to_int(row.get("active_connections")),
                "latency_ms": _to_float(row.get("latency_ms")),
                "congestion_score": _to_float(row.get("congestion_score")),
                "congestion_predicted_15min": bool(row.get("congestion_predicted_15min")),
                "avg_bandwidth_util": _to_float(row.get("avg_bandwidth_util")),
                "peak_bandwidth_util": _to_float(row.get("peak_bandwidth_util")),
                "bandwidth_trend_15min": str(row.get("bandwidth_trend_15min", "")),
            })

        seen = {t["tower_id"] for t in towers}
        for tid, (lat, lon) in TOWER_POSITIONS.items():
            if tid not in seen:
                towers.append({
                    "tower_id": tid,
                    "latitude": lat,
                    "longitude": lon,
                    "bandwidth_utilization_pct": 0,
                    "active_connections": 0,
                    "latency_ms": 0,
                    "congestion_score": 0,
                    "congestion_predicted_15min": False,
                    "avg_bandwidth_util": 0,
                    "peak_bandwidth_util": 0,
                    "bandwidth_trend_15min": "",
                })

        return {"towers": towers}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/customers")
def get_customers():
    """Return B2B business customer positions and upsell status for map dots."""
    try:
        rows = execute_query(f"""
            SELECT
                oc.customer_id,
                oc.company_name,
                oc.customer_type,
                oc.customer_segment,
                oc.conversion_score,
                oc.utilization_pct,
                oc.breach_risk_level,
                oc.tower_id,
                oc.contracted_bandwidth_mbps,
                oc.current_bandwidth_mbps,
                oc.monthly_revenue_opportunity,
                oc.monthly_contract_value
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities oc
            ORDER BY oc.conversion_score DESC
        """)

        customers = []
        for row in rows:
            tid = row.get("tower_id", "")
            base_lat, base_lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            seed = int(hashlib.md5(str(row["customer_id"]).encode()).hexdigest()[:8], 16)
            rng = random.Random(seed)
            offset_lat = rng.uniform(-0.0010, 0.0010)
            offset_lon = rng.uniform(-0.0010, 0.0010)
            show_proposal = _to_float(row.get("conversion_score")) > 0.65
            customers.append({
                "customer_id": str(row["customer_id"]),
                "company_name": str(row.get("company_name", row["customer_id"])),
                "customer_type": str(row.get("customer_type", "other")),
                "customer_segment": str(row.get("customer_segment", "other")),
                "latitude": base_lat + offset_lat,
                "longitude": base_lon + offset_lon,
                "conversion_score": _to_float(row.get("conversion_score")),
                "utilization_pct": _to_float(row.get("utilization_pct")),
                "breach_risk_level": str(row.get("breach_risk_level", "watch")),
                "tower_id": tid,
                "show_proposal": show_proposal,
                "contracted_bandwidth_mbps": _to_float(row.get("contracted_bandwidth_mbps")),
                "current_bandwidth_mbps": _to_float(row.get("current_bandwidth_mbps")),
                "monthly_revenue_opportunity": _to_float(row.get("monthly_revenue_opportunity")),
                "monthly_contract_value": _to_float(row.get("monthly_contract_value")),
            })

        return {"customers": customers}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/feed")
def get_feed():
    """Return last 20 events for the live feed panel."""
    try:
        proposals = execute_query(f"""
            SELECT customer_id, company_name, customer_type,
                   monthly_revenue_opportunity, accepted, sent_at,
                   utilization_pct, breach_risk_level
            FROM {CATALOG_SCHEMA}.upsell_proposal_log
            ORDER BY sent_at DESC
            LIMIT 15
        """)

        alerts = execute_query(f"""
            SELECT tower_id, window_start, avg_congestion_score,
                   peak_bandwidth_util, congestion_predicted_15min
            FROM {CATALOG_SCHEMA}.gold_congestion_features
            WHERE congestion_predicted_15min = 1
            ORDER BY window_start DESC
            LIMIT 8
        """)

        feed = []
        for p in proposals:
            icon = "accepted" if p.get("accepted") else "proposal"
            util = _to_float(p.get("utilization_pct", 0))
            company = p.get("company_name") or str(p["customer_id"])[:12]
            feed.append({
                "type": icon,
                "message": f"{'Accepted!' if p.get('accepted') else 'Proposal sent to'} {company} \u2014 {util:.0f}% util \u2014 +${_to_float(p.get('monthly_revenue_opportunity', 0)):.0f}/mo",
                "timestamp": str(p.get("sent_at", "")),
            })

        for a in alerts:
            feed.append({
                "type": "congestion",
                "message": f"Congestion predicted on {a['tower_id']} \u2014 Peak BW {_to_float(a.get('peak_bandwidth_util', 0)):.0f}% \u2014 Burst slice provisioned",
                "timestamp": str(a.get("window_start", "")),
            })

        feed.sort(key=lambda x: x["timestamp"], reverse=True)
        return {"feed": feed[:20]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/simulation/step/{step}")
def get_simulation_step(step: int):
    if step < 0 or step >= TOTAL_SIMULATION_STEPS:
        raise HTTPException(status_code=400, detail=f"Step must be 0-{TOTAL_SIMULATION_STEPS - 1}")

    try:
        time_range = execute_query(f"""
            SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
        """)

        if not time_range or not time_range[0].get("min_ts"):
            return _demo_simulation_step(step)

        towers_data = execute_query(f"""
            WITH time_bounds AS (
                SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts,
                       (UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))) / {TOTAL_SIMULATION_STEPS} AS step_seconds
                FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
            ),
            step_window AS (
                SELECT TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(min_ts) + {step} * step_seconds)) AS window_start,
                       TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(min_ts) + ({step} + 1) * step_seconds)) AS window_end
                FROM time_bounds
            )
            SELECT t.tower_id,
                   AVG(t.bandwidth_utilization_pct) AS bandwidth_utilization_pct,
                   MAX(t.active_connections) AS active_connections,
                   AVG(t.latency_ms) AS latency_ms,
                   AVG(t.bandwidth_utilization_pct) / 100.0 AS congestion_score
            FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry t
            CROSS JOIN step_window sw
            WHERE t.tower_id LIKE 'SEA-LF-%'
              AND t.timestamp >= sw.window_start
              AND t.timestamp < sw.window_end
            GROUP BY t.tower_id
        """)

        towers = []
        for row in towers_data:
            tid = row["tower_id"]
            lat, lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            bw_util = _to_float(row.get("bandwidth_utilization_pct"))
            towers.append({
                "tower_id": tid, "latitude": lat, "longitude": lon,
                "bandwidth_utilization_pct": bw_util,
                "active_connections": _to_int(row.get("active_connections")),
                "latency_ms": _to_float(row.get("latency_ms")),
                "congestion_score": bw_util / 100.0,
                "congestion_predicted_15min": bw_util > 70,
            })

        seen = {t["tower_id"] for t in towers}
        for tid, (lat, lon) in TOWER_POSITIONS.items():
            if tid not in seen:
                towers.append({
                    "tower_id": tid, "latitude": lat, "longitude": lon,
                    "bandwidth_utilization_pct": random.uniform(10, 30),
                    "active_connections": random.randint(50, 150),
                    "latency_ms": random.uniform(5, 15),
                    "congestion_score": random.uniform(0.1, 0.3),
                    "congestion_predicted_15min": False,
                })

        progress = (step + 1) / TOTAL_SIMULATION_STEPS
        customers_data = execute_query(f"""
            SELECT customer_id, company_name, customer_type, customer_segment,
                   conversion_score, utilization_pct, breach_risk_level,
                   tower_id, contracted_bandwidth_mbps, current_bandwidth_mbps,
                   monthly_revenue_opportunity, monthly_contract_value
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities
            ORDER BY conversion_score DESC
        """)

        customers = []
        for row in customers_data:
            tid = row.get("tower_id", "")
            base_lat, base_lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            seed = int(hashlib.md5(str(row["customer_id"]).encode()).hexdigest()[:8], 16)
            rng = random.Random(seed)
            offset_lat = rng.uniform(-0.0010, 0.0010)
            offset_lon = rng.uniform(-0.0010, 0.0010)
            conv_score = _to_float(row.get("conversion_score"))
            # Reveal step: seed-based so reveals spread from step 4 (pre-match) to step 21
            # Higher conv_score accounts reveal earlier via score-weighted seed
            score_offset = int((1.0 - min(1.0, max(0.0, conv_score))) * 10)
            reveal_step = 4 + ((seed + score_offset) % 18)
            show_proposal = step >= reveal_step
            customers.append({
                "customer_id": str(row["customer_id"]),
                "company_name": str(row.get("company_name", row["customer_id"])),
                "customer_type": str(row.get("customer_type", "other")),
                "customer_segment": str(row.get("customer_segment", "other")),
                "latitude": base_lat + offset_lat,
                "longitude": base_lon + offset_lon,
                "conversion_score": conv_score,
                "utilization_pct": _to_float(row.get("utilization_pct")),
                "breach_risk_level": str(row.get("breach_risk_level", "watch")),
                "tower_id": tid,
                "show_proposal": show_proposal,
                "contracted_bandwidth_mbps": _to_float(row.get("contracted_bandwidth_mbps")),
                "current_bandwidth_mbps": _to_float(row.get("current_bandwidth_mbps")),
                "monthly_revenue_opportunity": _to_float(row.get("monthly_revenue_opportunity")),
                "monthly_contract_value": _to_float(row.get("monthly_contract_value")),
            })

        kpi_proposals = execute_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.upsell_proposal_log")
        kpi_accepted  = execute_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.upsell_proposal_log WHERE accepted = true")
        kpi_arr = execute_query(f"SELECT projected_upsell_arr_usd, arr_protected_from_churn_usd FROM {CATALOG_SCHEMA}.event_revenue_summary ORDER BY summary_generated_at DESC LIMIT 1")
        kpi_bursts = execute_query(f"SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.bronze_network_slices WHERE status = 'active' AND slice_type LIKE '%_burst'")

        total_proposals = _to_int((kpi_proposals[0] if kpi_proposals else {}).get("cnt", 0))
        total_accepted  = _to_int((kpi_accepted[0] if kpi_accepted else {}).get("cnt", 0))
        total_upsell_arr = _to_float((kpi_arr[0] if kpi_arr else {}).get("projected_upsell_arr_usd", 0))
        total_arr_protected = _to_float((kpi_arr[0] if kpi_arr else {}).get("arr_protected_from_churn_usd", 0))
        total_bursts = _to_int((kpi_bursts[0] if kpi_bursts else {}).get("cnt", 0))
        congested_count = sum(1 for t in towers if t.get("congestion_predicted_15min"))

        kpis = {
            "accounts_at_risk": len(customers_data),
            "active_towers": len(towers),
            "towers_congested": congested_count,
            "proposals_sent": int(total_proposals * progress),
            "proposals_accepted": int(total_accepted * progress),
            "upsell_arr": total_upsell_arr * progress,
            "arr_protected": total_arr_protected * progress,
            "burst_slices": max(0, int(total_bursts * progress)),
        }

        feed_proposals = execute_query(f"""
            SELECT customer_id, company_name, monthly_revenue_opportunity, accepted, sent_at, utilization_pct
            FROM {CATALOG_SCHEMA}.upsell_proposal_log
            ORDER BY sent_at DESC
            LIMIT {min(12, max(3, int(step * 0.4)))}
        """)
        feed = []
        # Inject narrative events for the current match phase
        narrative = _get_narrative_events(step)
        feed.extend(narrative)
        for p in feed_proposals:
            icon = "accepted" if p.get("accepted") else "proposal"
            util = _to_float(p.get("utilization_pct", 0))
            company = p.get("company_name") or str(p["customer_id"])[:12]
            feed.append({
                "type": icon,
                "message": f"{'Accepted!' if p.get('accepted') else 'Proposal to'} {company} \u2014 {util:.0f}% util \u2014 +${_to_float(p.get('monthly_revenue_opportunity', 0)):.0f}/mo",
                "timestamp": str(p.get("sent_at", "")),
            })
        for t in towers:
            if t.get("congestion_predicted_15min"):
                feed.append({
                    "type": "congestion",
                    "message": f"Congestion on {t['tower_id']} \u2014 Score {t['congestion_score']:.2f} \u2014 Burst slice provisioned",
                    "timestamp": "",
                })

        timeline_labels = [
            "Pre-match", "Pre-match", "Pre-match", "Pre-match", "Pre-match",
            "Gates Open", "Gates Open", "Gates Open",
            "Kickoff 19:00", "Kickoff 19:00",
            "1st Half", "1st Half", "1st Half", "1st Half", "1st Half",
            "Halftime", "Halftime",
            "2nd Half", "2nd Half", "2nd Half", "2nd Half", "2nd Half",
            "2nd Half", "2nd Half",
            "T+73 GOAL!", "T+73 GOAL!",
            "Full Time", "Full Time", "Post-match", "Post-match",
        ]
        return {
            "step": step,
            "total_steps": TOTAL_SIMULATION_STEPS,
            "timeline_label": timeline_labels[step] if step < len(timeline_labels) else "Post-match",
            "towers": towers,
            "customers": customers,
            "kpis": kpis,
            "feed": feed[:15],
        }
    except Exception as e:
        print(f"Simulation query error: {e}")
        return _demo_simulation_step(step)


def _demo_simulation_step(step: int):
    """Generate demo data if database queries fail."""
    progress = (step + 1) / TOTAL_SIMULATION_STEPS
    rng = random.Random(step)

    towers = []
    for tid, (lat, lon) in TOWER_POSITIONS.items():
        # Goal spike at steps 24-25: towers near broadcaster cluster hit critical
        goal_spike = 1.4 if step in (24, 25) and tid in ("SEA-LF-001", "SEA-LF-002", "SEA-LF-003") else 1.0
        cong = min(0.99, rng.uniform(0.2, 0.85) * progress * goal_spike)
        towers.append({
            "tower_id": tid, "latitude": lat, "longitude": lon,
            "bandwidth_utilization_pct": min(99, rng.uniform(30, 90) * progress * goal_spike),
            "active_connections": int(rng.randint(100, 800) * progress),
            "latency_ms": rng.uniform(5, 50) * progress,
            "congestion_score": cong,
            "congestion_predicted_15min": cong > 0.68,
        })

    surge_accounts = {"BC-ESPN-001", "BC-FOX-001", "BC-APSN-001", "PM-TICK-001", "PM-SQR-001", "TM-USMNT-001"}
    customers = []
    for cid, company, ctype, tower in B2B_ACCOUNTS:
        base_lat, base_lon = TOWER_POSITIONS.get(tower, STADIUM_CENTER)
        # Fixed seed — no step component so dots don't move
        seed = int(hashlib.md5(cid.encode()).hexdigest()[:8], 16)
        crng = random.Random(seed)
        offset_lat = crng.uniform(-0.0008, 0.0008)
        offset_lon = crng.uniform(-0.0008, 0.0008)
        base_util = 0.60 if cid in surge_accounts else 0.45
        # Spike surge accounts harder at the goal
        goal_boost = 0.15 if step in (24, 25) and cid in surge_accounts else 0.0
        util_pct = min(99.0, (base_util + progress * 0.40 + goal_boost) * 100 + crng.uniform(-3, 3))
        conv_score = min(1.0, (util_pct - 70) / 30.0) if util_pct > 70 else 0.3
        # Reveal step: seed-based so reveals spread from step 4 (pre-match) to step 21
        score_offset = int((1.0 - min(1.0, max(0.0, conv_score))) * 10)
        reveal_step = 4 + ((seed + score_offset) % 18)
        show_proposal = step >= reveal_step
        customers.append({
            "customer_id": cid,
            "company_name": company,
            "customer_type": ctype,
            "customer_segment": _type_to_segment(ctype),
            "latitude": base_lat + offset_lat,
            "longitude": base_lon + offset_lon,
            "conversion_score": round(conv_score, 3),
            "utilization_pct": round(util_pct, 1),
            "breach_risk_level": "critical" if util_pct >= 90 else "warning" if util_pct >= 85 else "watch",
            "tower_id": tower,
            "show_proposal": show_proposal,
            "contracted_bandwidth_mbps": 300.0,
            "current_bandwidth_mbps": round(300.0 * util_pct / 100, 1),
            "monthly_revenue_opportunity": 1200.0,
            "monthly_contract_value": 5000.0,
        })

    timeline_labels = [
        "Pre-match", "Pre-match", "Pre-match", "Pre-match", "Pre-match",
        "Gates Open", "Gates Open", "Gates Open",
        "Kickoff 19:00", "Kickoff 19:00",
        "1st Half", "1st Half", "1st Half", "1st Half", "1st Half",
        "Halftime", "Halftime",
        "2nd Half", "2nd Half", "2nd Half", "2nd Half", "2nd Half",
        "2nd Half", "2nd Half",
        "T+73 GOAL!", "T+73 GOAL!",
        "Full Time", "Full Time", "Post-match", "Post-match",
    ]
    congested_count = sum(1 for t in towers if t["congestion_predicted_15min"])
    accounts_at_risk = sum(1 for c in customers if c["utilization_pct"] >= 85)
    proposals = int(min(18, accounts_at_risk) * progress)
    accepted  = int(proposals * 0.69)

    return {
        "step": step,
        "total_steps": TOTAL_SIMULATION_STEPS,
        "timeline_label": timeline_labels[step] if step < len(timeline_labels) else "Post-match",
        "towers": towers,
        "customers": customers,
        "kpis": {
            "accounts_at_risk": accounts_at_risk,
            "active_towers": 8,
            "towers_congested": congested_count,
            "proposals_sent": proposals,
            "proposals_accepted": accepted,
            "upsell_arr": 168000 * progress,
            "arr_protected": 2400000 * progress,
            "burst_slices": int(accounts_at_risk * progress),
        },
        "feed": _get_narrative_events(step),
    }


# ---------------------------------------------------------------------------
# Simulation narrative event log — step-aware, goal burst at step 24
# ---------------------------------------------------------------------------

# All (step, event) pairs. Events at the same step appear together.
_NARRATIVE_EVENTS = [
    (0,  "congestion", "All 8 Lumen Field towers online \u2014 Pre-match telemetry streaming"),
    (0,  "congestion", "SEA-LF-001 baseline BW: 12% \u2014 Monitoring active"),
    (2,  "proposal",   "Proposal to Ticketmaster Gates \u2014 Early entry scan surge: 41% util \u2014 +$1,200/mo"),
    (4,  "proposal",   "Proposal to SP+ Parking Pay \u2014 Lot scan load: 48% util \u2014 +$600/mo"),
    (5,  "congestion", "Gates opening \u2014 SEA-LF-001 BW climbing to 38%"),
    (5,  "proposal",   "Proposal to Square POS (merch) \u2014 Queue surge: 52% util \u2014 +$800/mo"),
    (5,  "proposal",   "Proposal to AXS Ticketing \u2014 Entry scan load: 55% util \u2014 +$1,200/mo"),
    (7,  "accepted",   "Accepted! AXS Ticketing \u2014 +200 Mbps burst slice provisioned"),
    (7,  "congestion", "SEA-LF-002 congestion predicted \u2014 Burst slice auto-provisioned"),
    (7,  "accepted",   "Accepted! Ticketmaster Gates \u2014 +300 Mbps \u2014 SLA protected"),
    (8,  "congestion", "KICKOFF \u2014 All broadcaster slices active \u2014 SEA-LF-001 at 61%"),
    (8,  "proposal",   "Proposal to ESPN \u2014 Live uplink rising: 68% util \u2014 +$2,000/mo"),
    (8,  "proposal",   "Proposal to Fox Sports \u2014 4K stream load: 65% util \u2014 +$1,800/mo"),
    (10, "accepted",   "Accepted! ESPN \u2014 +500 Mbps broadcast_uplink burst secured"),
    (10, "congestion", "SEA-LF-003 BW trend: +18%/15min \u2014 NWDAF alert \u2014 Burst provisioned"),
    (10, "proposal",   "Proposal to Apple TV+ Sports \u2014 Stream quality spike: 72% util \u2014 +$1,500/mo"),
    (12, "accepted",   "Accepted! Fox Sports \u2014 +400 Mbps \u2014 Broadcast quality secured"),
    (12, "proposal",   "Proposal to NBC Sports \u2014 Near SLA breach: 78% util \u2014 +$1,600/mo"),
    (12, "congestion", "SEA-LF-001 congestion in 15min \u2014 Pre-provisioning burst slice"),
    (14, "accepted",   "Accepted! Apple TV+ Sports \u2014 +500 Mbps burst active"),
    (14, "proposal",   "Proposal to TUDN/Univision \u2014 Spanish uplink: 74% util \u2014 +$1,400/mo"),
    (15, "congestion", "HALFTIME \u2014 Concession surge \u2014 Payment processors spiking"),
    (15, "proposal",   "Proposal to Aramark Kiosks N \u2014 Concession rush: 82% util \u2014 +$600/mo"),
    (15, "proposal",   "Proposal to Square POS (N) \u2014 Merch queue: 79% util \u2014 +$700/mo"),
    (15, "proposal",   "Proposal to Clover POS (VIP) \u2014 VIP lounge surge: 85% util \u2014 +$900/mo"),
    (15, "accepted",   "Accepted! Aramark Kiosks N \u2014 +150 Mbps burst slice active"),
    (16, "accepted",   "Accepted! NBC Sports \u2014 +350 Mbps uplink secured"),
    (16, "accepted",   "Accepted! TUDN/Univision \u2014 +400 Mbps Spanish broadcast secured"),
    (17, "congestion", "2ND HALF \u2014 SEA-LF-005 at 74% \u2014 NWDAF congestion alert issued"),
    (17, "proposal",   "Proposal to Butterfly AR App \u2014 AR stats feed spike: 77% util \u2014 +$1,100/mo"),
    (19, "accepted",   "Accepted! Clover POS (VIP) \u2014 +200 Mbps secured"),
    (20, "proposal",   "Proposal to Sportradar \u2014 Live data feed: 81% util \u2014 +$900/mo"),
    (20, "congestion", "SEA-LF-007 alert \u2014 Public safety slice protected, burst side-provisioned"),
    (22, "accepted",   "Accepted! Butterfly AR App \u2014 +250 Mbps AR feed secured"),
    (23, "congestion", "SEA-LF-001 at 88% \u2014 Broadcaster cluster approaching SLA breach"),
    (23, "proposal",   "Proposal to US Soccer Fed \u2014 Stats feed spike: 84% util \u2014 +$1,100/mo"),
    # ---- GOAL BURST at step 24 ----
    (24, "congestion", "\U0001f6a8 T+73 GOAL! SEA-LF-001 CRITICAL \u2014 97% BW \u2014 Emergency burst provisioned"),
    (24, "congestion", "\U0001f6a8 SEA-LF-002 CRITICAL \u2014 94% BW \u2014 NBC/Paramount burst active"),
    (24, "congestion", "\U0001f6a8 SEA-LF-003 ALERT \u2014 91% BW \u2014 Apple TV+ burst active"),
    (24, "congestion", "\U0001f6a8 SEA-LF-004 ALERT \u2014 88% BW \u2014 Burst slices provisioned"),
    (24, "proposal",   "URGENT: ESPN \u2014 GOAL spike 97% util \u2014 +$2,000/mo"),
    (24, "proposal",   "URGENT: Fox Sports \u2014 94% util \u2014 +$1,800/mo"),
    (24, "proposal",   "URGENT: Ticketmaster Gates \u2014 Replay surge 96% util \u2014 +$1,200/mo"),
    (24, "proposal",   "URGENT: Square POS (merch) \u2014 Souvenir rush 95% util \u2014 +$800/mo"),
    (24, "accepted",   "Accepted! ESPN \u2014 +600 Mbps burst \u2014 SLA protected \u2713"),
    (24, "accepted",   "Accepted! Fox Sports \u2014 +500 Mbps \u2014 4K stream secured \u2713"),
    (24, "accepted",   "Accepted! Ticketmaster Gates \u2014 +300 Mbps \u2014 Entry flow restored \u2713"),
    (24, "accepted",   "Accepted! Square POS (merch) \u2014 +250 Mbps \u2014 Souvenir sales live \u2713"),
    (25, "accepted",   "Accepted! US Soccer Fed \u2014 +200 Mbps post-goal analytics burst"),
    (25, "accepted",   "Accepted! Sportradar \u2014 +150 Mbps live stats delivery secured"),
    (25, "congestion", "Post-goal: All 8 towers stable \u2014 Burst slices holding \u2014 SLA maintained"),
    (26, "congestion", "FULL TIME \u2014 Final whistle \u2014 Load dispersing across towers"),
    (26, "accepted",   "Accepted! Aramark Kiosks S \u2014 +100 Mbps post-match concession burst"),
    (28, "congestion", "Event complete \u2014 13 burst slices active \u2014 $17,040/mo upsell secured"),
    (29, "accepted",   "Accepted! Cardtronics ATMs \u2014 Post-match cash surge \u2014 +$400/mo"),
]


def _get_narrative_events(step: int) -> list:
    """Return all narrative events that have occurred up to and including `step`, newest first."""
    events = [
        {"type": etype, "message": msg, "timestamp": ""}
        for s, etype, msg in _NARRATIVE_EVENTS
        if s <= step
    ]
    # Reverse so most recent (highest step) events appear first
    events.reverse()
    return events[:20]


def _type_to_segment(ctype: str) -> str:
    return {
        "broadcaster": "media_rights",
        "venue_operator": "venue_ops",
        "public_safety": "critical_services",
        "payment_processor": "payments_ticketing",
        "team_sponsor": "teams_sponsors",
    }.get(ctype, "other")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _to_int(val, default=0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Serve React SPA
# ---------------------------------------------------------------------------
frontend_dist = Path(__file__).parent / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        static_file = frontend_dist / full_path
        if static_file.is_file():
            return FileResponse(static_file)
        return FileResponse(frontend_dist / "index.html")
else:
    @app.get("/")
    async def root():
        return {"message": "Frontend not built. Run 'npm run build' in frontend/ directory."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
