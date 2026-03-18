# Dynamic Network Slicing for Live Events — TelcoMax Demo

## Brand Guidelines
- **Company**: TelcoMax (Tier-1 wireless carrier, 45M subscribers)
- **Primary persona**: Sarah Kim, VP of Network Operations
- **Color scheme**: Telecom blue (#003087), signal green (#00A651), alert red (#D7263D)
- **Tone**: Executive-ready, ROI-focused, data-driven

## Use Case
AI-powered 5G network slicing platform that predicts congestion 15 minutes ahead and autonomously
provisions premium slices during live events (USMNT vs Australia at Lumen Field, Seattle),
converting network stress into revenue.

## Business Impact
- **$6.2M ARR** from single event conversions
- **Zero SLA violations** (vs. $2.1M historical penalties)
- **47% conversion rate** on premium plan upgrades
- **30-second** autonomous slice provisioning (vs. 45-60 min manual)
- **23% improvement** in customer satisfaction scores

## Architecture
```
Cell Tower Telemetry (bronze_cell_tower_telemetry — data generation)
Customer Profiles    (bronze_customer_profiles   — data generation)
Event Calendar       (bronze_event_calendar      — data generation)
Network Slices       (bronze_network_slices      — data generation)
         ↓
  Spark Declarative Pipeline (SDP)
         ↓
Silver Layer
  - silver_tower_metrics          (cleaned telemetry + congestion score)
  - silver_customer_enriched      (segmentation + upgrade propensity)
  - silver_event_tower_proximity  (Haversine geospatial joins)
         ↓
Gold Layer
  - gold_congestion_features      (15-min trend windows, NWDAF features)
  - gold_conversion_opportunities (real-time customer conversion scoring)
  - gold_revenue_analytics        (aggregated revenue projections)
         ↓
ML Models (registered in Unity Catalog via MLflow)
  - telcomax_congestion_predictor (RandomForest — 15-min ahead)
  - telcomax_customer_scorer      (GBM — premium conversion propensity)
         ↓
┌─ Network Operations Dashboard (AI/BI Lakeview)
├─ Revenue Genie Space (natural language analytics)
└─ Autonomous Slice Provisioning Job
```

## Workspace & Catalog
- **Workspace**: https://fevm-cmegdemos.cloud.databricks.com
- **Profile**: fevm-cmegdemos
- **Catalog**: cmegdemos_catalog
- **Schema**: dynamic_slicing_live_event
- **Python**: 3.11 → serverless client "2", databricks-connect==15.4.5

## Key Demo Moments
1. Pre-event: normal traffic ~25% bandwidth, dashboard all green
2. T-15 min: NWDAF detects rising trend, congestion flags fire on SEA-LF towers
3. System auto-provisions 3,200 premium slices in <30 seconds
4. 47% of targeted customers accept $15/month upgrade offer
5. T+73 min — USMNT scores: 847% traffic spike, zero drops for premium tier
6. Genie Space: "How much revenue did we generate from the USMNT match?" → $271K / $6.2M ARR
