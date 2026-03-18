# Telco Live Event Splicing

AI-powered 5G network slicing demo that predicts congestion 15 minutes ahead and autonomously provisions premium slices during live events—turning network stress into revenue.

## Overview

**TelcoMax** (Tier-1 wireless carrier, 45M subscribers) uses this platform to handle traffic spikes during major events (e.g., USMNT vs Australia at Lumen Field). The system predicts congestion, identifies high-value conversion targets, and provisions premium 5G slices in under 30 seconds instead of 45–60 minutes manually.

### Business impact

| Metric | Result |
|--------|--------|
| **ARR from single event** | $6.2M |
| **SLA violations** | Zero (vs. $2.1M historical penalties) |
| **Premium upgrade conversion** | 47% |
| **Autonomous slice provisioning** | ~30 seconds (vs. 45–60 min manual) |
| **Customer satisfaction** | +23% |

## Architecture

```
Cell Tower Telemetry, Customer Profiles, Event Calendar, Network Slices (bronze)
         ↓
  Spark Declarative Pipeline (SDP)
         ↓
Silver: tower_metrics, customer_enriched, event_tower_proximity
         ↓
Gold: congestion_features, conversion_opportunities, revenue_analytics
         ↓
ML (Unity Catalog / MLflow): congestion_predictor, customer_scorer
         ↓
Lakeview Dashboard · Revenue Genie Space · Autonomous Slice Provisioning Job
```

- **Bronze**: Raw telemetry, customer profiles, event calendar, slice configs (from data generation jobs).
- **Silver**: Cleaned metrics + congestion scoring, customer segmentation, event–tower geospatial joins.
- **Gold**: 15-min trend features (NWDAF-style), real-time conversion scoring, revenue aggregations.
- **Models**: RandomForest (15-min ahead congestion), GBM (premium conversion propensity).
- **Outputs**: Network Operations dashboard (Lakeview), Revenue Genie Space, and an autonomous slice provisioning job.

## Repository structure

```
├── databricks.yml              # Databricks Asset Bundle (workspace, catalog, schema)
├── resources/                  # Bundle resources: pipelines, jobs, Genie space
├── src/
│   ├── app/                    # Databricks App (FastAPI + React frontend)
│   ├── pipeline/transformations # Bronze → silver → gold SQL
│   ├── models/                 # Congestion predictor, customer scorer
│   ├── jobs/                   # Autonomous slice provisioning
│   ├── dashboards/             # Lakeview dashboard JSON
│   └── data_generation/        # Synthetic telemetry, customers, events
├── dynamic-network-slicing-live-events/  # Storyline, walkthrough, schema docs
├── pyproject.toml / uv.lock    # Python dependencies
└── DEMO.md                     # Demo notes and key moments
```

## Prerequisites

- **Python 3.11+** (e.g. [uv](https://docs.astral.sh/uv/))
- **Databricks workspace** and CLI configured (e.g. `databricks auth login`)
- **Node.js** (for the app frontend, if building locally)

## Setup

1. **Clone and install Python dependencies**

   ```bash
   git clone https://github.com/maxcarduner1/telco_live_event_splicing.git
   cd telco_live_event_splicing
   uv sync
   ```

2. **Configure the bundle**

   Edit `databricks.yml` if needed (workspace host, catalog, schema). Defaults:

   - Catalog: `cmegdemos_catalog`
   - Schema: `dynamic_slicing_live_event`

3. **Deploy with Databricks Asset Bundle**

   ```bash
   databricks bundle deploy -t dev
   ```

4. **Run data generation and pipeline**

   Use the deployed jobs (e.g. data generation, telemetry pipeline, model training, slice automation) from the Databricks workspace or via the bundle.

5. **App (optional)**

   The `src/app` directory is a Databricks App (FastAPI backend + React frontend). Deploy and run it from the workspace, or run locally:

   ```bash
   cd src/app
   pip install -r requirements.txt
   cd frontend && npm install && npm run build
   cd .. && uvicorn main:app --host 0.0.0.0 --port 8000
   ```

## Key demo moments

1. **Pre-event**: Normal traffic; dashboard green.
2. **T−15 min**: NWDAF-style models flag rising congestion on event towers.
3. **Auto-provisioning**: ~3,200 premium slices in &lt;30 seconds.
4. **Conversion**: 47% of targeted customers accept premium upgrade.
5. **Peak (e.g. goal)**: 847% traffic spike; zero drops for premium tier.
6. **Genie**: “How much revenue did we generate from the USMNT match?” → $271K / $6.2M ARR.

## Documentation

- **DEMO.md** — Brand, use case, architecture, workspace/catalog, demo flow.
- **dynamic-network-slicing-live-events/** — Storyline, technical storyline, project structure, data schema, walkthrough.

## License

See repository license file.
