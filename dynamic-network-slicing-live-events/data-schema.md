# Data Schema

## Table Schemas

### Bronze Layer (Raw Ingestion)

#### bronze_cell_tower_telemetry
Real-time network performance metrics from 1,200 cell towers across TelcoMax's network.

| Column | Type | Description |
|--------|------|-------------|
| tower_id | STRING | Unique identifier for cell tower (e.g., "SEA-LF-001") |
| timestamp | TIMESTAMP | Metric collection timestamp (1-second intervals) |
| latitude | DOUBLE | Tower GPS latitude |
| longitude | DOUBLE | Tower GPS longitude |
| active_connections | BIGINT | Current number of active connections |
| bandwidth_utilization_pct | DOUBLE | Percentage of total tower bandwidth used (0–100) |
| signal_strength_dbm | DOUBLE | Average signal strength in dBm (−120 to −30) |
| latency_ms | DOUBLE | Average round-trip latency in milliseconds |
| packet_loss_pct | DOUBLE | Packet loss percentage (0–100) |
| throughput_mbps | DOUBLE | Current throughput in Mbps |
| error_rate_pct | DOUBLE | Connection error rate percentage |
| temperature_celsius | DOUBLE | Tower equipment temperature |
| power_consumption_watts | DOUBLE | Current power consumption |

**Source**: Streaming from Network Management System (NMS)
**Row Count**: ~50M (1,200 towers × 86,400 seconds/day × retention period)
**Distribution**: CLUSTER BY (tower_id, timestamp)

---

#### bronze_customer_profiles
B2B business customer contract registry — the five major stadium customer groups.

| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Unique customer identifier (e.g., "BC-ESPN-001") |
| company_name | STRING | Business name (e.g., "ESPN") |
| customer_type | STRING | Segment: broadcaster \| venue_operator \| public_safety \| payment_processor \| team_sponsor |
| contract_tier | STRING | Contract level: standard \| gold \| platinum |
| monthly_contract_value | DOUBLE | Monthly recurring revenue from this account |
| contracted_bandwidth_mbps | DOUBLE | Guaranteed bandwidth in the SLA (Mbps) |
| contracted_latency_ms | DOUBLE | Maximum latency guaranteed in SLA |
| sla_tier | STRING | SLA level: standard \| gold \| platinum |
| churn_risk_score | DOUBLE | ML-generated churn probability (0–1) |
| contract_start_date | DATE | Contract effective date |
| contract_renewal_months | INT | Months until next renewal decision |
| support_tickets_30d | INT | Support tickets opened in the last 30 days |
| peak_event_utilization_pct | DOUBLE | Historical peak utilization fraction during events (0–1) |
| account_manager_id | STRING | Assigned account manager |

**Source**: CRM and BSS systems
**Row Count**: ~35 (one row per active stadium business account)
**Distribution**: CLUSTER BY (customer_id)

---

#### bronze_event_calendar
Major events that drive network traffic spikes.

| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Unique event identifier |
| event_name | STRING | Event name (e.g., "USMNT vs Australia") |
| venue_name | STRING | Venue name (e.g., "Lumen Field") |
| venue_latitude | DOUBLE | Venue GPS latitude |
| venue_longitude | DOUBLE | Venue GPS longitude |
| event_start_time | TIMESTAMP | Event start time |
| event_end_time | TIMESTAMP | Event end time |
| expected_attendance | INT | Projected attendance |
| event_type | STRING | Category (sports, concert, conference) |
| traffic_multiplier | DOUBLE | Expected traffic increase vs. baseline |

**Source**: Event management APIs and manual entry
**Row Count**: ~2K
**Distribution**: CLUSTER BY (event_start_time)

---

#### bronze_network_slices
Active and historical network slice configurations for B2B customers, including real-time utilization snapshots.

| Column | Type | Description |
|--------|------|-------------|
| slice_id | STRING | Unique slice identifier |
| customer_id | STRING | Associated B2B customer |
| tower_id | STRING | Serving tower |
| slice_type | STRING | broadcast_uplink \| venue_operations \| public_safety \| payment_processing \| team_operations |
| contracted_bandwidth_mbps | DOUBLE | Bandwidth guaranteed in the SLA |
| bandwidth_allocated_mbps | DOUBLE | Bandwidth currently allocated (≥ contracted) |
| current_bandwidth_mbps | DOUBLE | Actual throughput at snapshot time |
| utilization_pct | DOUBLE | current / contracted × 100 — key breach signal |
| latency_guarantee_ms | DOUBLE | Maximum latency SLA |
| created_timestamp | TIMESTAMP | Slice creation time |
| expires_timestamp | TIMESTAMP | Slice expiration time |
| status | STRING | active \| provisioning \| terminated |
| revenue_per_hour | DOUBLE | Hourly revenue from this slice |

**Source**: Network slice orchestrator + autonomous provisioning job
**Row Count**: ~250 active + ~200 historical
**Distribution**: CLUSTER BY (tower_id, created_timestamp)

---

## Relationships

- `bronze_customer_profiles.customer_id` → `bronze_network_slices.customer_id` (1:many)
- `bronze_cell_tower_telemetry.tower_id` → `bronze_network_slices.tower_id` (1:many)
- Geographic proximity joins between towers and events based on lat/lng coordinates
- Time-based joins between telemetry and events for congestion correlation

---

## Transformations

### Bronze → Silver (Cleaning and Enrichment)

```sql
-- Silver: cleaned and standardized telemetry
CREATE OR REFRESH STREAMING TABLE silver_tower_metrics AS
SELECT
  tower_id,
  timestamp,
  latitude,
  longitude,
  GREATEST(0, active_connections) AS active_connections,
  LEAST(100, GREATEST(0, bandwidth_utilization_pct)) AS bandwidth_utilization_pct,
  CASE
    WHEN signal_strength_dbm BETWEEN -120 AND -30 THEN signal_strength_dbm
    ELSE NULL
  END AS signal_strength_dbm,
  GREATEST(0, latency_ms) AS latency_ms,
  LEAST(100, GREATEST(0, packet_loss_pct)) AS packet_loss_pct,
  GREATEST(0, throughput_mbps) AS throughput_mbps,
  LEAST(100, GREATEST(0, error_rate_pct)) AS error_rate_pct,
  temperature_celsius,
  power_consumption_watts,
  -- Congestion score calculation
  (bandwidth_utilization_pct * 0.4 +
   packet_loss_pct * 0.3 +
   (latency_ms / 100) * 0.2 +
   error_rate_pct * 0.1) AS congestion_score
FROM STREAM(bronze_cell_tower_telemetry)
WHERE tower_id IS NOT NULL
  AND timestamp IS NOT NULL
  AND bandwidth_utilization_pct IS NOT NULL;
```

```sql
-- Silver: B2B customer profiles with segmentation and upsell propensity
CREATE OR REFRESH STREAMING TABLE silver_customer_enriched AS
SELECT
  c.customer_id,
  c.company_name,
  c.customer_type,
  UPPER(c.contract_tier)              AS contract_tier,
  c.monthly_contract_value,
  c.contracted_bandwidth_mbps,
  c.contracted_latency_ms,
  UPPER(c.sla_tier)                   AS sla_tier,
  c.churn_risk_score,
  c.contract_start_date,
  c.contract_renewal_months,
  c.support_tickets_30d,
  c.peak_event_utilization_pct,
  c.account_manager_id,
  -- B2B segment by customer type
  CASE
    WHEN c.customer_type = 'broadcaster'        THEN 'media_rights'
    WHEN c.customer_type = 'public_safety'      THEN 'critical_services'
    WHEN c.customer_type = 'venue_operator'     THEN 'venue_ops'
    WHEN c.customer_type = 'payment_processor'  THEN 'payments_ticketing'
    WHEN c.customer_type = 'team_sponsor'       THEN 'teams_sponsors'
    ELSE 'other'
  END AS customer_segment,
  -- Upsell propensity score (0-1)
  CASE
    WHEN c.churn_risk_score > 0.70                                           THEN 0.15
    WHEN c.contract_renewal_months <= 3
         AND c.peak_event_utilization_pct >= 0.80                            THEN 0.90
    WHEN c.contract_renewal_months <= 6
         AND c.peak_event_utilization_pct >= 0.75                            THEN 0.80
    WHEN c.support_tickets_30d >= 3                                          THEN 0.85
    WHEN c.peak_event_utilization_pct >= 0.85                                THEN 0.75
    WHEN c.peak_event_utilization_pct >= 0.70                                THEN 0.60
    ELSE 0.40
  END AS upsell_propensity_score,
  -- Revenue opportunity: 50% bandwidth expansion
  ROUND(c.contracted_bandwidth_mbps * 0.50 * 0.008 * 1000, 2)
    AS monthly_upsell_opportunity_usd
FROM STREAM(bronze_customer_profiles) c
WHERE c.customer_id IS NOT NULL;
```

```sql
-- Silver: event-tower proximity mapping
CREATE OR REFRESH MATERIALIZED VIEW silver_event_tower_proximity AS
SELECT
  e.event_id,
  e.event_name,
  e.venue_name,
  e.event_start_time,
  e.event_end_time,
  e.expected_attendance,
  e.traffic_multiplier,
  t.tower_id,
  t.latitude AS tower_latitude,
  t.longitude AS tower_longitude,
  SQRT(
    POW(69.1 * (e.venue_latitude - t.latitude), 2) +
    POW(69.1 * (e.venue_longitude - t.longitude) * COS(e.venue_latitude / 57.3), 2)
  ) AS distance_miles
FROM bronze_event_calendar e
CROSS JOIN (
  SELECT DISTINCT tower_id, latitude, longitude
  FROM bronze_cell_tower_telemetry
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
) t
WHERE SQRT(
  POW(69.1 * (e.venue_latitude - t.latitude), 2) +
  POW(69.1 * (e.venue_longitude - t.longitude) * COS(e.venue_latitude / 57.3), 2)
) <= 5.0;
```

---

### Silver → Gold (Analytics and Features)

```sql
-- Gold: NWDAF congestion prediction features (unchanged)
CREATE OR REFRESH STREAMING TABLE gold_congestion_features AS
...  -- (see silver→gold section in technical-storyline.md)
```

```sql
-- Gold: B2B bandwidth breach and upsell opportunity scoring
CREATE OR REFRESH MATERIALIZED VIEW gold_conversion_opportunities AS
WITH latest_congestion AS (
  SELECT tower_id, avg_congestion_score, bandwidth_trend_15min,
         congestion_predicted_15min, near_event_flag, window_start,
         ROW_NUMBER() OVER (PARTITION BY tower_id ORDER BY window_start DESC) AS rn
  FROM LIVE.gold_congestion_features
  WHERE congestion_predicted_15min = 1 OR avg_congestion_score > 60
),
active_slices AS (
  SELECT customer_id, tower_id, slice_id, contracted_bandwidth_mbps,
         current_bandwidth_mbps, utilization_pct, latency_guarantee_ms, revenue_per_hour,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY utilization_pct DESC) AS rn
  FROM bronze_network_slices
  WHERE status = 'active'
)
SELECT
  c.customer_id, c.company_name, c.customer_type, c.customer_segment,
  c.contract_tier, c.monthly_contract_value, c.contracted_bandwidth_mbps,
  c.upsell_propensity_score, c.monthly_upsell_opportunity_usd, c.churn_risk_score,
  c.contract_renewal_months, c.account_manager_id,
  s.slice_id, s.tower_id, s.current_bandwidth_mbps, s.utilization_pct,
  lc.avg_congestion_score, lc.congestion_predicted_15min,
  CASE
    WHEN s.utilization_pct >= 100 THEN 'breach'
    WHEN s.utilization_pct >=  90 THEN 'critical'
    WHEN s.utilization_pct >=  85 THEN 'warning'
    ELSE 'watch'
  END AS breach_risk_level,
  ROUND(
    (c.upsell_propensity_score * 0.45 +
     LEAST(1.0, GREATEST(0.0, (s.utilization_pct - 75.0) / 25.0)) * 0.35 +
     CASE WHEN lc.congestion_predicted_15min = 1 THEN 0.20 ELSE 0.0 END),
    4
  ) AS conversion_score,
  c.monthly_upsell_opportunity_usd AS monthly_revenue_opportunity,
  current_timestamp() AS scored_at
FROM LIVE.silver_customer_enriched c
JOIN active_slices s ON c.customer_id = s.customer_id AND s.rn = 1
JOIN latest_congestion lc ON s.tower_id = lc.tower_id AND lc.rn = 1
WHERE s.utilization_pct >= 85
   OR (s.utilization_pct >= 75 AND lc.congestion_predicted_15min = 1);
```

```sql
-- Gold: B2B revenue analytics aggregations
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_analytics AS
SELECT
  DATE(scored_at)                           AS analysis_date,
  EXTRACT(HOUR FROM scored_at)              AS hour_of_day,
  customer_segment,
  COUNT(*)                                  AS total_customers_at_risk,
  COUNT(CASE WHEN breach_risk_level = 'breach'   THEN 1 END) AS breach_count,
  COUNT(CASE WHEN breach_risk_level = 'critical' THEN 1 END) AS critical_count,
  COUNT(CASE WHEN conversion_score > 0.70 THEN 1 END)        AS high_upsell_opportunities,
  ROUND(AVG(conversion_score), 4)           AS avg_upsell_score,
  ROUND(SUM(monthly_revenue_opportunity), 2) AS total_monthly_upsell_opportunity,
  ROUND(SUM(CASE WHEN conversion_score > 0.70 THEN monthly_revenue_opportunity ELSE 0 END) * 0.62 * 12, 2)
                                            AS projected_upsell_arr,
  ROUND(SUM(CASE WHEN churn_risk_score > 0.25 AND conversion_score > 0.60
                 THEN monthly_contract_value * 12 ELSE 0 END), 2)
                                            AS arr_at_churn_risk,
  ROUND(AVG(utilization_pct), 2)            AS avg_utilization_pct,
  ROUND(MAX(utilization_pct), 2)            AS peak_utilization_pct
FROM LIVE.gold_conversion_opportunities
GROUP BY DATE(scored_at), EXTRACT(HOUR FROM scored_at), customer_segment;
```

---

## Output Tables (Written by Autonomous Job)

### upsell_proposal_log
One row per upsell proposal sent to a business account or account manager.

| Column | Type | Description |
|--------|------|-------------|
| proposal_id | STRING | Unique proposal identifier |
| customer_id | STRING | Target B2B customer |
| company_name | STRING | Business name |
| customer_type | STRING | Customer segment type |
| account_manager_id | STRING | Account manager notified |
| proposal_message | STRING | Personalized upsell offer text |
| sent_at | TIMESTAMP | Proposal delivery time |
| conversion_score | DOUBLE | Upsell urgency score (0–1) |
| utilization_pct | DOUBLE | Bandwidth utilization at time of proposal |
| breach_risk_level | STRING | breach / critical / warning / watch |
| contract_renewal_months | INT | Months until renewal |
| churn_risk_score | DOUBLE | Churn probability |
| accepted | BOOLEAN | Whether the account accepted the expansion |
| monthly_revenue_opportunity | DOUBLE | Monthly upsell revenue if accepted |
| annual_upsell_contribution | DOUBLE | ARR from this accepted upsell |
| monthly_contract_value | DOUBLE | Existing monthly contract value (churn protection reference) |

### event_revenue_summary
Single-row event P&L written at the end of each match simulation.

| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Event identifier |
| event_name | STRING | Event name |
| event_date | DATE | Match date |
| venue_name | STRING | Venue |
| b2b_customers_targeted | BIGINT | Accounts receiving upsell proposals |
| b2b_customers_accepted | BIGINT | Accounts that accepted capacity expansion |
| acceptance_rate | DOUBLE | Acceptance fraction |
| immediate_monthly_upsell_usd | DOUBLE | Total new monthly revenue from accepted upsells |
| projected_upsell_arr_usd | DOUBLE | Annualized new ARR from expanded contracts |
| arr_protected_from_churn_usd | DOUBLE | Existing ARR protected by proactive service delivery |
| burst_slices_provisioned | BIGINT | Number of burst capacity slices auto-provisioned |
| sla_violations | INT | Count of contractual SLA breaches (target: 0) |
| summary_generated_at | TIMESTAMP | Summary generation timestamp |
