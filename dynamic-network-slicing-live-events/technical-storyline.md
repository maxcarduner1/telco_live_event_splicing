# Technical Storyline: Dynamic Network Slicing for Live Events

## Audience
Engineering leaders, data architects, platform engineers, and ML engineers evaluating Databricks for real-time AI workloads in telecommunications.

---

## The Technical Challenge

TelcoMax ingests telemetry from 1,200 cell towers at 1-second intervals — roughly 50 million records per day. The goal is to take that raw operational data and, within a 15-minute window, predict which towers are about to congest, identify which B2B business customers (broadcasters, venue operators, security teams, payment processors, and team/sponsor groups) are approaching their contracted bandwidth limits, provision burst capacity, and trigger personalized upsell proposals to account managers — all without a human in the loop.

This is a pipeline that spans streaming ingestion, feature engineering, ML inference, automated actuation, and business analytics. The challenge isn't any one of those problems — it's doing all of them on a single platform, at low latency, with production reliability. The key new dimension versus consumer-grade congestion management: every flagged customer has a *contracted* bandwidth ceiling, a churn risk, and a renewal date — all of which must be incorporated into the real-time scoring.

---

## Product-by-Product Technical Walkthrough

### 1. Delta Lake + Unity Catalog — The Data Foundation

**What it does here**: All four source domains land as external Delta tables in Unity Catalog under `cmegdemos_catalog.dynamic_slicing_live_event`:

| Table | Source | Volume |
|-------|--------|--------|
| `bronze_cell_tower_telemetry` | NMS streaming feed | ~50M rows/day |
| `bronze_customer_profiles` | CRM/BSS systems | 35 B2B accounts (broadcasters, venue ops, security, payments, teams) |
| `bronze_event_calendar` | Event management API | ~2K rows |
| `bronze_network_slices` | Slice orchestrator | ~250 active slices; `contracted_bandwidth_mbps` + `utilization_pct` are new key columns |

Delta's key role here is **dual-mode access**: the same tables are read by streaming pipelines, written to by batch generation jobs, queried by BI dashboards, and appended to by the autonomous provisioning job — all transactionally consistent without coordination overhead.

**Liquid Clustering** is applied on `(tower_id, timestamp)` for telemetry and `(tower_id, created_timestamp)` for slices, giving the range scans in the gold layer and dashboard queries predictable I/O without managing partition schemes manually.

**Unity Catalog** provides a single governance layer across all assets: tables, ML models, and functions share the same three-level namespace (`catalog.schema.asset`), so the provisioning job writing to `bronze_network_slices` and the model registered as `cmegdemos_catalog.dynamic_slicing_live_event.telcomax_congestion_predictor` are in the same access control domain.

---

### 2. Spark Declarative Pipelines (SDP) — The Medallion ETL Engine

**What it does here**: Six SQL files define the entire bronze→silver→gold transformation graph. The pipeline runs serverless on the `PREVIEW` channel with `spark.databricks.delta.schema.autoMerge.enabled = true`.

#### Silver Layer — Cleaning and Enrichment

**`silver_tower_metrics`** — a **Streaming Table** that consumes `STREAM(bronze_cell_tower_telemetry)` and does three things:
- Bounds-checks all KPIs (bandwidth 0–100%, signal strength -120 to -30 dBm)
- Drops rows with null `tower_id` or `timestamp`
- Computes an inline **congestion score** using a weighted KPI formula:
  ```
  congestion_score = bandwidth_util × 0.4 + packet_loss × 0.3
                   + (latency_ms / 100) × 0.2 + error_rate × 0.1
  ```

**`silver_customer_enriched`** — a **Streaming Table** on `bronze_customer_profiles` that adds two derived columns computed in-SQL:
- `customer_segment` — B2B segmentation (`media_rights`, `critical_services`, `venue_ops`, `payments_ticketing`, `teams_sponsors`) derived from `customer_type`
- `upsell_propensity_score` — a business-rule score ranging 0.15–0.90 based on churn risk, contract renewal urgency (`contract_renewal_months`), historical peak utilization (`peak_event_utilization_pct`), and support ticket volume. Accounts within 3 months of renewal AND above 80% peak utilization score 0.90.
- `monthly_upsell_opportunity_usd` — computed inline as `contracted_bandwidth_mbps × 0.50 × $0.008/Mbps`, representing the monthly revenue from a 50% bandwidth expansion offer

**`silver_event_tower_proximity`** — a **Materialized View** that CROSS JOINs the event calendar against every distinct tower position and filters to towers within 5 miles using the Haversine approximation:
```sql
SQRT(POW(69.1 * (venue_lat - tower_lat), 2)
   + POW(69.1 * (venue_lon - tower_lon) * COS(venue_lat / 57.3), 2)) <= 5.0
```
This produces the geospatial lookup that lets the gold layer know which towers are in-scope for a given event without recomputing it at query time.

#### Gold Layer — NWDAF Features and Scoring

**`gold_congestion_features`** — a **Streaming Table** that applies a tumbling 5-minute window over `silver_tower_metrics`, then computes **lag-based trend indicators**:
```sql
AVG(bandwidth_util) - LAG(AVG(bandwidth_util), 1) OVER (
  PARTITION BY tower_id ORDER BY window_start
) AS bandwidth_trend_15min
```
The `near_event_flag` is computed via a correlated EXISTS subquery against `silver_event_tower_proximity` on both tower and time range — this is what connects network telemetry to the event calendar.

**`gold_conversion_opportunities`** — a **Materialized View** (B2B upsell opportunities) that joins three sources:
```
silver_customer_enriched
  JOIN active_slices (highest-utilization slice per customer)  ON customer_id
  JOIN latest_congestion (most recent 5-min window per tower)  ON tower_id
```
The join produces a per-customer `conversion_score` as a weighted sum of upsell propensity, utilization pressure, and congestion prediction:
```
conversion_score = upsell_propensity_score × 0.45
                 + utilization_pressure (LEAST(1, (utilization_pct−75)/25)) × 0.35
                 + 0.20 if congestion_predicted_15min = 1
```
Only B2B accounts with `utilization_pct >= 85` OR (`utilization_pct >= 75` AND `congestion_predicted_15min = 1`) pass the filter. This keeps the output tightly scoped to actionable accounts. The `breach_risk_level` field (`breach` / `critical` / `warning` / `watch`) provides a human-readable urgency label for NOC dashboards and account-manager notifications.

**`gold_revenue_analytics`** — a **Materialized View** that rolls up upsell opportunities hourly by `customer_segment`, computing:
- `projected_upsell_arr` using 62% B2B acceptance rate
- `arr_at_churn_risk` for accounts with `churn_risk_score > 0.25` and high upsell scores — the churn-prevention value TelcoMax protects by proactive service delivery

**Why SDP over standard Spark Structured Streaming?** Pipeline-level dependency resolution means the execution order of these six tables is inferred from the `STREAM(...)` references — no DAG hand-wiring. Schema auto-merge handles the evolving slice schema as the provisioning job appends new columns. And the pipeline produces its own lineage graph visible in the workspace UI.

---

### 3. MLflow + Unity Catalog Model Registry — Training and Tracking

**What it does here**: Two scikit-learn models are trained via Databricks Jobs using `spark_python_task` on serverless compute, logged to MLflow, and registered directly into Unity Catalog.

#### Model 1: `telcomax_congestion_predictor`
- **Algorithm**: `RandomForestClassifier` (sklearn)
- **Features** (11): `avg_bandwidth_util`, `peak_bandwidth_util`, `avg_connections`, `avg_congestion_score`, `avg_latency_ms`, `avg_packet_loss_pct`, `bandwidth_trend_15min`, `connection_trend_15min`, `near_event_flag`, `hour_of_day`, `day_of_week`
- **Label**: `congestion_predicted_15min` (binary: will this tower congest in the next 15 min?)
- **Training data**: `gold_congestion_features` (2,750 rows of windowed telemetry)
- **MLflow tracking**: accuracy, precision, recall, ROC-AUC, feature importances, and the fitted `StandardScaler` are all logged as artifacts under `/Shared/telcomax_nwdaf_experiments`

#### Model 2: `telcomax_customer_scorer`
- **Algorithm**: `GradientBoostingClassifier` (sklearn)
- **Features** (10): B2B contract metrics (`monthly_contract_value`, `contracted_bandwidth_mbps`, `contract_renewal_months`, `churn_risk_score`) + live utilization context (`utilization_pct`, `avg_congestion_score`, `congestion_predicted_15min`, `near_event_flag`) + segment and tier encodings
- **Label**: `will_accept_upsell` (simulated from `conversion_score` with 62% base rate + customer-type adjustments: broadcasters 72%, payment processors 68%, public safety 50%)
- **Training data**: `gold_conversion_opportunities` joined with B2B customer features (~120 rows of event-day snapshots)

Both models are registered via:
```python
mlflow.set_registry_uri("databricks-uc")
mlflow.sklearn.log_model(
    model,
    artifact_path="model",
    registered_model_name="cmegdemos_catalog.dynamic_slicing_live_event.telcomax_congestion_predictor"
)
```
The UC-native registration means the model is immediately accessible to any compute in the catalog with `USE` privilege — no separate model registry endpoint required. Version 1 of both models is in `READY` status.

---

### 4. Databricks Jobs — Orchestration and Autonomous Actuation

**What it does here**: Three jobs manage the end-to-end pipeline lifecycle, all defined in Asset Bundles YAML and deployed with `databricks bundle deploy`.

#### Data Generation Job
Four parallel `spark_python_task` tasks (events → customers → telemetry, network slices) running on serverless with `client: "2"` (Databricks Connect). Tasks `generate_telemetry` and `generate_network_slices` depend on their respective upstream tasks, enforcing write order. Environment spec pins `numpy>=1.24.0,<2.0` to avoid PyArrow binary incompatibility in the serverless runtime.

#### Model Training Job
Sequential tasks: `train_congestion_predictor` → `train_customer_scorer` (customer scorer depends on the congestion features the first model is trained on). Uses a separate `ml_env` environment spec with scikit-learn and MLflow pinned. Both tasks use `spark_python_task` on serverless — the scripts detect `DATABRICKS_RUNTIME_VERSION` and call `SparkSession.builder.getOrCreate()` vs `DatabricksSession` accordingly.

#### Autonomous Slice Provisioning Job (The Actuator)
This is the most technically interesting job — it is pure Spark SQL with no Python ML inference at runtime. At execution:

1. **Loads B2B targets**: queries `gold_conversion_opportunities` for `conversion_score >= 0.65` — B2B accounts at bandwidth breach risk
2. **Provisions burst capacity**: `INSERT INTO bronze_network_slices` — writes one burst slice per targeted account with `slice_type = '{customer_type}_burst'`, `contracted_bandwidth_mbps = existing × 0.50`, `utilization_pct = 60.0` (fresh headroom), `status = 'active'`, 4-hour expiry
3. **Logs upsell proposals**: `INSERT INTO upsell_proposal_log` — records per-account proposal with personalized message (utilization %, tower ID, offer price), acceptance outcome (customer-type-stratified probability: broadcasters 72%, payment processors 68%), and annual upsell contribution
4. **Writes summary**: `INSERT OVERWRITE event_revenue_summary` — single-row event P&L covering upsell ARR and churn-prevention ARR

The burst slice write back into `bronze_network_slices` closes the loop — the SDP pipeline picks up those new rows on its next micro-batch, dashboard slice counts update, and utilization pressure on original slices drops as traffic is redistributed.

---

### 5. AI/BI (Lakeview) Dashboard — Real-Time Operations View

**What it does here**: A Lakeview dashboard (`network_operations.lvdash.json`) deployed via Asset Bundle provides the real-time NOC view for Sarah Kim's team. It reads directly from the gold-layer Delta tables — no intermediate serving layer.

Key panels:
- **Tower health grid**: per-tower `avg_bandwidth_util` and `congestion_score` heatmap from `gold_congestion_features`
- **Congestion predictions**: towers with `congestion_predicted_15min = 1` flagged 15 minutes ahead
- **Active slices**: count of `status = 'active'` rows in `bronze_network_slices` by tower and slice type
- **Revenue pipeline**: running totals from `event_revenue_summary` and `gold_revenue_analytics`

Lakeview's **auto-refresh** capability is what makes this a live dashboard during the demo — it re-executes the underlying SQL on the warehouse at configurable intervals without any streaming connector.

---

### 6. Genie Space — Natural Language Analytics

**What it does here**: A Genie Space is configured over four tables — `gold_revenue_analytics`, `gold_conversion_opportunities`, `event_revenue_summary`, and `sms_campaign_log` — giving business users a natural language query layer on top of the same Delta tables the pipeline writes to.

Representative queries the system can answer out-of-the-box:
- *"How much new ARR did we generate from the USMNT match?"* → queries `event_revenue_summary.projected_upsell_arr_usd`
- *"How much ARR did we protect from churn at this event?"* → queries `event_revenue_summary.arr_protected_from_churn_usd`
- *"Which customer types accept upsell offers most often during sporting events?"* → aggregates `upsell_proposal_log` by `customer_type`
- *"Which broadcast accounts are approaching their bandwidth limit and renewing within 6 months?"* → joins `gold_conversion_opportunities` with `silver_customer_enriched` on renewal and utilization filters
- *"What's our total B2B upsell pipeline ARR across all events this month?"* → sums `projected_upsell_arr` from `gold_revenue_analytics`

Genie works directly on Delta — there is no ETL into a separate semantic layer. The same table that the provisioning job writes to 30 seconds ago is queryable by a VP in plain English.

---

### 7. Databricks Asset Bundles — Infrastructure as Code

**What it does here**: The entire demo is deployed from a single `databricks.yml` with target environments (`dev`, `prod`). Every resource — pipelines, jobs, dashboards, Genie space — is declared in YAML and deployed atomically with `databricks bundle deploy`.

```
resources/
  telemetry_pipeline.pipeline.yml     # SDP config with serverless + PREVIEW channel
  data_generation.job.yml             # 4-task parallel data gen with env pinning
  model_training.job.yml              # Sequential ML training with ml_env spec
  slice_automation.job.yml            # Actuation job with serverless_env
  revenue_genie_space.genie.yml       # Genie Space table binding
```

The bundle handles workspace path resolution (`${workspace.file_path}`), catalog/schema parameterization (`${var.catalog}`), and environment-specific naming (`[${bundle.target}]` prefix on all resource names). `dev` and `prod` targets share the same resource definitions with different variable bindings — no YAML duplication.

---

## End-to-End Data Flow

```
NMS / CRM / Events / Slice Orchestrator
         │
         ▼ (batch write or streaming append)
    Bronze Delta Tables  ◄──────────────────────────────────┐
    (Unity Catalog)                                          │
         │                                                   │
         ▼ STREAM(...)                                       │
  Spark Declarative Pipeline                                 │
    silver_tower_metrics          ← cleaned telemetry       │
    silver_customer_enriched      ← segmentation + propensity│
    silver_event_tower_proximity  ← Haversine geospatial MV │
         │                                                   │
         ▼ window() + LAG()                                  │
    gold_congestion_features      ← NWDAF 15-min predictions │
    gold_conversion_opportunities ← 3-stream join + scoring  │
    gold_revenue_analytics        ← hourly revenue rollup MV │
         │                                                   │
         ▼ toPandas() → sklearn fit → mlflow.log_model()     │
  Unity Catalog Model Registry                               │
    telcomax_congestion_predictor  (RandomForest)            │
    telcomax_customer_scorer       (GradientBoosting)        │
         │                                                   │
         ▼ gold_conversion_opportunities WHERE score >= 0.70 │
  Autonomous Slice Provisioning Job ──── INSERT INTO ────────┘
    → bronze_network_slices (new premium slices)
    → sms_campaign_log (offer + conversion outcome)
    → event_revenue_summary (event P&L)
         │
         ▼
  AI/BI Lakeview Dashboard    ← NOC real-time ops view
  Genie Space                 ← NL revenue analytics
```

---

## Key Technical Differentiators

| Capability | How Databricks Delivers It |
|------------|---------------------------|
| Sub-30s burst slice provisioning | SDP streaming + Jobs actuation writing directly to Delta — no message queue, no microservice hop |
| 15-min congestion + utilization prediction | LAG window functions over streaming aggregates (tower-level) + `utilization_pct` ratio on bronze slices feeding into gold breach scoring |
| B2B-aware scoring | Silver layer encodes contract renewal urgency and historical peak utilization; gold layer produces a composite upsell score accounting for both bandwidth pressure and commercial propensity |
| Single governance plane | Unity Catalog covers B2B customer contracts, slice telemetry, ML models, and output proposal tables under one `catalog.schema` namespace |
| No separate serving infrastructure | NOC dashboard and Genie query gold-layer Delta tables directly — warehouse-as-serving-layer, no ETL into a separate BI semantic layer |
| IaC deployment | Asset Bundles deploy the entire stack (pipelines, jobs, dashboards, Genie) from one `databricks bundle deploy` |
| Closed-loop actuation | Provisioning job reads breach-scored B2B accounts, writes burst slices back to `bronze_network_slices`, and logs proposals to `upsell_proposal_log` — all in the same catalog, re-readable by the pipeline on its next micro-batch |
