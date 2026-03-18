"""
TelcoMax Dynamic 5G Network Slicing - Live Event Visualization
FastAPI backend serving data from Databricks SQL Warehouse
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

# Simulation: 30 steps representing the match timeline
TOTAL_SIMULATION_STEPS = 30


def _get_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "https://fevm-cmegdemos.cloud.databricks.com")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _get_token() -> str:
    """Get auth token - Apps OAuth (DATABRICKS_CLIENT_ID/SECRET), env var, or SDK."""
    # Databricks Apps: use service principal OAuth via SDK (preferred)
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config
        cfg = Config()
        headers = cfg.authenticate()
        if headers and "Authorization" in headers:
            return headers["Authorization"].replace("Bearer ", "")
    except Exception:
        pass
    # Fallback: static token env var
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    raise RuntimeError("No auth token available. Configure DATABRICKS_TOKEN or deploy via Databricks Apps.")


def get_connection():
    """Create a fresh Databricks SQL connection."""
    return databricks_sql.connect(
        server_hostname=_get_host().replace("https://", ""),
        http_path=WAREHOUSE_HTTP_PATH,
        access_token=_get_token(),
    )


def execute_query(sql: str, params=None):
    """Execute SQL and return list of dicts."""
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
    # Warm-up: verify connectivity
    try:
        execute_query("SELECT 1")
        print("[OK] Databricks SQL connection verified")
    except Exception as e:
        print(f"[WARN] Databricks SQL connection test failed: {e}")
    yield


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(title="TelcoMax Live Event", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/api/kpis")
def get_kpis():
    """Return all KPI numbers for the top bar."""
    try:
        # Customers near venue
        r1 = execute_query(f"""
            SELECT COUNT(DISTINCT customer_id) AS cnt
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities
            WHERE near_event_flag = 1
        """)
        customers_near = r1[0]["cnt"] if r1 else 0

        # Active towers
        r2 = execute_query(f"""
            SELECT COUNT(DISTINCT tower_id) AS cnt
            FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
        """)
        active_towers = r2[0]["cnt"] if r2 else 0

        # Towers in congestion
        r3 = execute_query(f"""
            SELECT COUNT(DISTINCT tower_id) AS cnt
            FROM {CATALOG_SCHEMA}.gold_congestion_features
            WHERE congestion_predicted_15min = 1
        """)
        towers_congested = r3[0]["cnt"] if r3 else 0

        # Offers sent
        r4 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.sms_campaign_log
        """)
        offers_sent = r4[0]["cnt"] if r4 else 0

        # Converted
        r5 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.sms_campaign_log
            WHERE converted = true
        """)
        converted = r5[0]["cnt"] if r5 else 0

        # Projected ARR
        r6 = execute_query(f"""
            SELECT projected_arr_usd
            FROM {CATALOG_SCHEMA}.event_revenue_summary
            ORDER BY summary_generated_at DESC
            LIMIT 1
        """)
        projected_arr = float(r6[0]["projected_arr_usd"]) if r6 else 0

        # Active premium slices
        r7 = execute_query(f"""
            SELECT COUNT(*) AS cnt
            FROM {CATALOG_SCHEMA}.bronze_network_slices
            WHERE status = 'active' AND slice_type = 'premium_streaming'
        """)
        active_slices = r7[0]["cnt"] if r7 else 0

        return {
            "customers_near": customers_near,
            "active_towers": active_towers,
            "towers_congested": towers_congested,
            "offers_sent": offers_sent,
            "converted": converted,
            "projected_arr": projected_arr,
            "active_slices": active_slices,
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

        # Fill in any missing towers with defaults
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
    """Return customer positions scattered around towers for map dots."""
    try:
        rows = execute_query(f"""
            SELECT
                customer_id,
                customer_segment,
                conversion_score,
                near_event_flag,
                tower_id,
                avg_congestion_score,
                congestion_predicted_15min,
                monthly_revenue_opportunity
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities
            WHERE near_event_flag = 1
            ORDER BY conversion_score DESC
            LIMIT 200
        """)

        customers = []
        for row in rows:
            tid = row.get("tower_id", "")
            base_lat, base_lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)

            # Deterministic scatter based on customer_id
            seed = int(hashlib.md5(str(row["customer_id"]).encode()).hexdigest()[:8], 16)
            rng = random.Random(seed)
            offset_lat = rng.uniform(-0.0015, 0.0015)
            offset_lon = rng.uniform(-0.0015, 0.0015)

            show_offer = (
                _to_float(row.get("conversion_score")) > 0.7
                and row.get("near_event_flag") == 1
            )

            customers.append({
                "customer_id": str(row["customer_id"]),
                "customer_segment": str(row.get("customer_segment", "standard")),
                "latitude": base_lat + offset_lat,
                "longitude": base_lon + offset_lon,
                "conversion_score": _to_float(row.get("conversion_score")),
                "tower_id": tid,
                "show_offer": show_offer,
                "monthly_revenue_opportunity": _to_float(row.get("monthly_revenue_opportunity")),
            })

        return {"customers": customers}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/feed")
def get_feed():
    """Return last 20 events for the live feed panel."""
    try:
        # Recent offers
        offers = execute_query(f"""
            SELECT
                customer_id,
                customer_segment,
                subscription_tier,
                monthly_revenue_opportunity,
                converted,
                sent_at
            FROM {CATALOG_SCHEMA}.sms_campaign_log
            ORDER BY sent_at DESC
            LIMIT 15
        """)

        # Recent congestion alerts
        alerts = execute_query(f"""
            SELECT
                tower_id,
                window_start,
                avg_congestion_score,
                peak_bandwidth_util,
                congestion_predicted_15min
            FROM {CATALOG_SCHEMA}.gold_congestion_features
            WHERE congestion_predicted_15min = 1
            ORDER BY window_start DESC
            LIMIT 10
        """)

        feed = []
        for o in offers:
            icon = "trophy" if o.get("converted") else "offer"
            feed.append({
                "type": icon,
                "message": f"{'Converted!' if o.get('converted') else 'Offer sent to'} Customer {str(o['customer_id'])[:8]}... ({o.get('customer_segment', 'standard')}) - ${_to_float(o.get('monthly_revenue_opportunity', 0)):.0f}/mo",
                "timestamp": str(o.get("sent_at", "")),
            })

        for a in alerts:
            feed.append({
                "type": "congestion",
                "message": f"Congestion predicted on {a['tower_id']} - Peak BW {_to_float(a.get('peak_bandwidth_util', 0)):.0f}% - Slice provisioned",
                "timestamp": str(a.get("window_start", "")),
            })

        # Sort by timestamp descending, take 20
        feed.sort(key=lambda x: x["timestamp"], reverse=True)
        return {"feed": feed[:20]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/simulation/step/{step}")
def get_simulation_step(step: int):
    """
    Return a data snapshot for simulation step 0-29.
    We partition the telemetry timestamps into 30 windows and return data
    for the requested window, plus progressive customer reveals.
    """
    if step < 0 or step >= TOTAL_SIMULATION_STEPS:
        raise HTTPException(status_code=400, detail=f"Step must be 0-{TOTAL_SIMULATION_STEPS - 1}")

    try:
        # Get the time range of the data
        time_range = execute_query(f"""
            SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
            FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
        """)

        if not time_range or not time_range[0].get("min_ts"):
            return _demo_simulation_step(step)

        min_ts = time_range[0]["min_ts"]
        max_ts = time_range[0]["max_ts"]

        # Get tower data for this time window
        towers_data = execute_query(f"""
            WITH time_bounds AS (
                SELECT
                    MIN(timestamp) AS min_ts,
                    MAX(timestamp) AS max_ts,
                    (UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))) / {TOTAL_SIMULATION_STEPS} AS step_seconds
                FROM {CATALOG_SCHEMA}.bronze_cell_tower_telemetry
            ),
            step_window AS (
                SELECT
                    TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(min_ts) + {step} * step_seconds)) AS window_start,
                    TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(min_ts) + ({step} + 1) * step_seconds)) AS window_end
                FROM time_bounds
            )
            SELECT
                t.tower_id,
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

        # Build tower list with positions
        towers = []
        for row in towers_data:
            tid = row["tower_id"]
            lat, lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            bw_util = _to_float(row.get("bandwidth_utilization_pct"))
            towers.append({
                "tower_id": tid,
                "latitude": lat,
                "longitude": lon,
                "bandwidth_utilization_pct": bw_util,
                "active_connections": _to_int(row.get("active_connections")),
                "latency_ms": _to_float(row.get("latency_ms")),
                "congestion_score": bw_util / 100.0,
                "congestion_predicted_15min": bw_util > 70,
            })

        # Fill missing towers
        seen = {t["tower_id"] for t in towers}
        for tid, (lat, lon) in TOWER_POSITIONS.items():
            if tid not in seen:
                towers.append({
                    "tower_id": tid,
                    "latitude": lat,
                    "longitude": lon,
                    "bandwidth_utilization_pct": random.uniform(10, 30),
                    "active_connections": random.randint(50, 150),
                    "latency_ms": random.uniform(5, 15),
                    "congestion_score": random.uniform(0.1, 0.3),
                    "congestion_predicted_15min": False,
                })

        # Progressive customer reveals: step 0 shows ~20, step 29 shows ~200
        customer_count = min(200, max(20, int(20 + (step / (TOTAL_SIMULATION_STEPS - 1)) * 180)))
        customers_data = execute_query(f"""
            SELECT
                customer_id,
                customer_segment,
                conversion_score,
                near_event_flag,
                tower_id,
                monthly_revenue_opportunity
            FROM {CATALOG_SCHEMA}.gold_conversion_opportunities
            WHERE near_event_flag = 1
            ORDER BY conversion_score DESC
            LIMIT {customer_count}
        """)

        customers = []
        for row in customers_data:
            tid = row.get("tower_id", "")
            base_lat, base_lon = TOWER_POSITIONS.get(tid, STADIUM_CENTER)
            seed = int(hashlib.md5(str(row["customer_id"]).encode()).hexdigest()[:8], 16)
            rng = random.Random(seed)

            # Customers converge toward stadium as simulation progresses
            progress = step / max(1, TOTAL_SIMULATION_STEPS - 1)
            spread = 0.0020 * (1 - progress * 0.6)
            offset_lat = rng.uniform(-spread, spread)
            offset_lon = rng.uniform(-spread, spread)

            conv_score = _to_float(row.get("conversion_score"))
            # Show offers progressively in later steps for high-score customers
            show_offer = conv_score > 0.7 and step >= 10 and rng.random() < progress

            customers.append({
                "customer_id": str(row["customer_id"]),
                "customer_segment": str(row.get("customer_segment", "standard")),
                "latitude": base_lat + offset_lat,
                "longitude": base_lon + offset_lon,
                "conversion_score": conv_score,
                "tower_id": tid,
                "show_offer": show_offer,
                "monthly_revenue_opportunity": _to_float(row.get("monthly_revenue_opportunity")),
            })

        # Build KPIs that progress with the simulation
        base_offers = max(1, int(step / TOTAL_SIMULATION_STEPS * 200))
        base_converted = max(0, int(base_offers * 0.35))

        # Fetch actual counts scaled to step
        kpi_offers = execute_query(f"""
            SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.sms_campaign_log
        """)
        kpi_converted = execute_query(f"""
            SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.sms_campaign_log WHERE converted = true
        """)
        kpi_arr = execute_query(f"""
            SELECT projected_arr_usd FROM {CATALOG_SCHEMA}.event_revenue_summary
            ORDER BY summary_generated_at DESC LIMIT 1
        """)
        kpi_slices = execute_query(f"""
            SELECT COUNT(*) AS cnt FROM {CATALOG_SCHEMA}.bronze_network_slices
            WHERE status = 'active' AND slice_type = 'premium_streaming'
        """)

        total_offers = _to_int((kpi_offers[0] if kpi_offers else {}).get("cnt", 0))
        total_converted = _to_int((kpi_converted[0] if kpi_converted else {}).get("cnt", 0))
        total_arr = _to_float((kpi_arr[0] if kpi_arr else {}).get("projected_arr_usd", 0))
        total_slices = _to_int((kpi_slices[0] if kpi_slices else {}).get("cnt", 0))

        # Scale KPIs to step progress
        progress = (step + 1) / TOTAL_SIMULATION_STEPS
        congested_count = sum(1 for t in towers if t.get("congestion_predicted_15min"))

        kpis = {
            "customers_near": customer_count,
            "active_towers": len(towers),
            "towers_congested": congested_count,
            "offers_sent": int(total_offers * progress),
            "converted": int(total_converted * progress),
            "projected_arr": total_arr * progress,
            "active_slices": max(0, int(total_slices * progress)),
        }

        # Feed events for this step
        feed_offers = execute_query(f"""
            SELECT customer_id, customer_segment, subscription_tier,
                   monthly_revenue_opportunity, converted, sent_at
            FROM {CATALOG_SCHEMA}.sms_campaign_log
            ORDER BY sent_at DESC
            LIMIT {min(15, max(3, int(step * 0.5)))}
        """)

        feed = []
        for o in feed_offers:
            icon = "trophy" if o.get("converted") else "offer"
            feed.append({
                "type": icon,
                "message": f"{'Converted!' if o.get('converted') else 'Offer sent to'} Customer {str(o['customer_id'])[:8]}... ({o.get('customer_segment', 'standard')}) - ${_to_float(o.get('monthly_revenue_opportunity', 0)):.0f}/mo",
                "timestamp": str(o.get("sent_at", "")),
            })

        for t in towers:
            if t.get("congestion_predicted_15min"):
                feed.append({
                    "type": "congestion",
                    "message": f"Congestion predicted on {t['tower_id']} - Score {t['congestion_score']:.2f} - Slice provisioned",
                    "timestamp": "",
                })

        # Match timeline label
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
        timeline_label = timeline_labels[step] if step < len(timeline_labels) else "Post-match"

        return {
            "step": step,
            "total_steps": TOTAL_SIMULATION_STEPS,
            "timeline_label": timeline_label,
            "towers": towers,
            "customers": customers,
            "kpis": kpis,
            "feed": feed[:15],
        }
    except Exception as e:
        # Fallback to demo data on error
        print(f"Simulation query error: {e}")
        return _demo_simulation_step(step)


def _demo_simulation_step(step: int):
    """Generate demo data if database queries fail."""
    progress = (step + 1) / TOTAL_SIMULATION_STEPS
    rng = random.Random(step)

    towers = []
    for tid, (lat, lon) in TOWER_POSITIONS.items():
        cong = rng.uniform(0.2, 0.95) * progress
        towers.append({
            "tower_id": tid,
            "latitude": lat,
            "longitude": lon,
            "bandwidth_utilization_pct": rng.uniform(30, 95) * progress,
            "active_connections": int(rng.randint(100, 800) * progress),
            "latency_ms": rng.uniform(5, 50) * progress,
            "congestion_score": cong,
            "congestion_predicted_15min": cong > 0.7,
        })

    customers = []
    customer_count = int(20 + progress * 180)
    segments = ["high_value_influencer", "high_value", "influencer", "standard", "premium"]
    for i in range(customer_count):
        tid = list(TOWER_POSITIONS.keys())[rng.randint(0, 7)]
        base_lat, base_lon = TOWER_POSITIONS[tid]
        spread = 0.0020 * (1 - progress * 0.6)
        conv_score = rng.uniform(0.3, 1.0)
        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_segment": rng.choice(segments),
            "latitude": base_lat + rng.uniform(-spread, spread),
            "longitude": base_lon + rng.uniform(-spread, spread),
            "conversion_score": conv_score,
            "tower_id": tid,
            "show_offer": conv_score > 0.7 and step >= 10 and rng.random() < progress,
            "monthly_revenue_opportunity": rng.uniform(15, 85),
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
    offers = int(320 * progress)
    converted = int(offers * 0.34)

    return {
        "step": step,
        "total_steps": TOTAL_SIMULATION_STEPS,
        "timeline_label": timeline_labels[step] if step < len(timeline_labels) else "Post-match",
        "towers": towers,
        "customers": customers,
        "kpis": {
            "customers_near": customer_count,
            "active_towers": 8,
            "towers_congested": congested_count,
            "offers_sent": offers,
            "converted": converted,
            "projected_arr": 148000 * progress,
            "active_slices": int(12 * progress),
        },
        "feed": [
            {"type": "offer", "message": f"Offer sent to CUST-{rng.randint(1000,9999)} (high_value) - $45/mo", "timestamp": ""},
            {"type": "congestion", "message": f"Congestion on SEA-LF-00{rng.randint(1,8)} - Slice provisioned", "timestamp": ""},
            {"type": "trophy", "message": f"Converted! CUST-{rng.randint(1000,9999)} (influencer) - $65/mo", "timestamp": ""},
        ],
    }


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
        # Don't catch API routes
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        # Try to serve static file first
        static_file = frontend_dist / full_path
        if static_file.is_file():
            return FileResponse(static_file)
        # Fallback to index.html for SPA routing
        return FileResponse(frontend_dist / "index.html")
else:
    @app.get("/")
    async def root():
        return {"message": "Frontend not built. Run 'npm run build' in frontend/ directory."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
