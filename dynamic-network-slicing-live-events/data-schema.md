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
| active_connections | BIGINT | Current number of active user connections |
| bandwidth_utilization_pct | DOUBLE | Percentage of total bandwidth currently used (0-100) |
| signal_strength_dbm | DOUBLE | Average signal strength in dBm (-120 to -30) |
| latency_ms | DOUBLE | Average round-trip latency in milliseconds |
| packet_loss_pct | DOUBLE | Packet loss percentage (0-100) |
| throughput_mbps | DOUBLE | Current throughput in Mbps |
| error_rate_pct | DOUBLE | Connection error rate percentage |
| temperature_celsius | DOUBLE | Tower equipment temperature |
| power_consumption_watts | DOUBLE | Current power consumption |

**Source**: Streaming from Network Management System (NMS)  
**Row Count**: ~50M (1,200 towers × 86,400 seconds/day × retention period)  
**Distribution**: CLUSTER BY (tower_id, timestamp)

#### bronze_customer_profiles
Customer subscription details, demographics, and behavioral scoring.

| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Unique customer identifier |
| subscription_tier | STRING | Current plan (basic, premium, enterprise) |
| monthly_revenue | DOUBLE | Monthly recurring revenue from customer |
| account_start_date | DATE | Customer acquisition date |
| age | INT | Customer age |
| location_zip | STRING | Primary residence ZIP code |
| device_type | STRING | Primary device (iPhone, Samsung, etc.) |
| social_influence_score | INT | Social media follower count (0-1M+) |
| data_usage_gb_monthly | DOUBLE | Average monthly data consumption |
| sla_tier | STRING | Service level agreement (standard, gold, platinum) |
| churn_risk_score | DOUBLE | ML-generated churn probability (0-1) |
| last_upgrade_date | DATE | Most recent plan upgrade |
| support_tickets_30d | INT | Support tickets in last 30 days |

**Source**: CRM and BSS systems  
**Row Count**: ~45M  
**Distribution**: CLUSTER BY (customer_id)

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
| traffic_multiplier | DOUBLE | Expected traffic increase (1.0 = baseline) |

**Source**: Event management APIs and manual entry  
**Row Count**: ~2K  
**Distribution**: CLUSTER BY (event_start_time)

#### bronze_network_slices
Active network slice configurations and performance.

| Column | Type | Description |
|--------|------|-------------|
| slice_id | STRING | Unique slice identifier |
| customer_id | STRING | Associated customer |
| tower_id | STRING | Serving tower |
| slice_type | STRING | Slice category (premium_streaming, enterprise_iot, etc.) |
| bandwidth_allocated_mbps | DOUBLE | Dedicated bandwidth allocation |
| latency_guarantee_ms | DOUBLE | Maximum latency SLA |
| created_timestamp | TIMESTAMP | Slice creation time |
| expires_timestamp | TIMESTAMP | Slice expiration time |
| status | STRING | Current status (active, provisioning, terminated) |
| revenue_per_hour | DOUBLE | Hourly revenue from this slice |

**Source**: Network slice orchestrator  
**Row Count**: ~500K  
**Distribution**: CLUSTER BY (tower_id, created_timestamp)

## Relationships

- `bronze_customer_profiles.customer_id` → `bronze_network_slices.customer_id` (1:many)
- `bronze_cell_tower_telemetry.tower_id` → `bronze_network_slices.tower_id` (1:many)
- Geographic proximity joins between towers and events based on lat/lng coordinates
- Time-based joins between telemetry and events for congestion correlation

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
-- Silver: enriched customer profiles with geospatial tower mapping
CREATE OR REFRESH STREAMING TABLE silver_customer_enriched AS
SELECT
  c.customer_id,
  UPPER(c.subscription_tier) AS subscription_tier,
  c.monthly_revenue,
  c.account_start_date,
  c.age,
  c.location_zip,
  c.device_type,
  c.social_influence_score,
  c.data_usage_gb_monthly,
  UPPER(c.sla_tier) AS sla_tier,
  c.churn_risk_score,
  c.last_upgrade_date,
  c.support_tickets_30d,
  -- Customer value scoring
  CASE
    WHEN c.monthly_revenue >= 100 AND c.social_influence_score >= 10000 THEN 'high_value_influencer'
    WHEN c.monthly_revenue >= 100 THEN 'high_value'
    WHEN c.social_influence_score >= 10000 THEN 'influencer'
    WHEN c.subscription_tier = 'PREMIUM' THEN 'premium'
    ELSE 'standard'
  END AS customer_segment,
  -- Upgrade propensity score
  CASE
    WHEN c.churn_risk_score > 0.7 THEN 0.1
    WHEN c.last_upgrade_date IS NULL OR datediff(current_date(), c.last_upgrade_date) > 365 THEN 0.8
    WHEN c.support_tickets_30d > 3 THEN 0.9
    ELSE 0.5
  END AS upgrade_propensity_score
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
  -- Calculate distance using Haversine formula approximation
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
) <= 5.0;  -- Within 5 miles of event venue
```

### Silver → Gold (Analytics and Features)

```sql
-- Gold: NWDAF congestion prediction features
CREATE OR REFRESH STREAMING TABLE gold_congestion_features AS
SELECT
  tower_id,
  window_start,
  window_end,
  -- Current metrics
  AVG(bandwidth_utilization_pct) AS avg_bandwidth_util,
  MAX(bandwidth_utilization_pct) AS peak_bandwidth_util,
  AVG(active_connections) AS avg_connections,
  MAX(active_connections) AS peak_connections,
  AVG(congestion_score) AS avg_congestion_score,
  -- Trend indicators (15-minute windows)
  AVG(bandwidth_utilization_pct) - LAG(AVG(bandwidth_utilization_pct), 1) OVER (
    PARTITION BY tower_id ORDER BY window_start
  ) AS bandwidth_trend_15min,
  AVG(active_connections) - LAG(AVG(active_connections), 1) OVER (
    PARTITION BY tower_id ORDER BY window_start
  ) AS connection_trend_15min,
  -- Event proximity indicator
  CASE 
    WHEN EXISTS (
      SELECT 1 FROM silver_event_tower_proximity ep
      WHERE ep.tower_id = tm.tower_id
        AND window_start BETWEEN ep.event_start_time - INTERVAL 2 HOURS 
                              AND ep.event_end_time + INTERVAL 1 HOUR
    ) THEN 1 ELSE 0
  END AS near_event_flag,
  -- Time-based features
  EXTRACT(HOUR FROM window_start) AS hour_of_day,
  EXTRACT(DOW FROM window_start) AS day_of_week,
  -- Congestion prediction (simplified rule-based for demo)
  CASE
    WHEN AVG(bandwidth_utilization_pct) > 75 
         AND (AVG(bandwidth_utilization_pct) - LAG(AVG(bandwidth_utilization_pct), 1) OVER (
           PARTITION BY tower_id ORDER BY window_start
         )) > 10 THEN 1
    ELSE 0
  END AS congestion_predicted_15min
FROM (
  SELECT 
    tower_id,
    window(timestamp, '5 minutes') AS window,
    bandwidth_utilization_pct,
    active_connections,
    congestion_score
  FROM STREAM(silver_tower_metrics)
) tm
GROUP BY tower_id, window.start, window.end;
```

```sql
-- Gold: customer conversion opportunities
CREATE OR REFRESH STREAMING TABLE gold_conversion_opportunities AS
SELECT
  c.customer_id,
  c.customer_segment,
  c.monthly_revenue,
  c.upgrade_propensity_score,
  c.social_influence_score,
  ns.slice_id,
  ns.tower_id,
  cf.avg_congestion_score,
  cf.congestion_predicted_15min,
  -- Conversion scoring
  (c.upgrade_propensity_score * 0.4 +
   CASE WHEN c.customer_segment IN ('high_value_influencer', 'influencer') THEN 0.3 ELSE 0.1 END +
   CASE WHEN cf.congestion_predicted_15min = 1 THEN 0.3 ELSE 0.0 END) AS conversion_score,
  -- Revenue opportunity
  CASE c.subscription_tier
    WHEN 'BASIC' THEN 25.0  -- Upgrade to premium
    WHEN 'PREMIUM' THEN 50.0  -- Upgrade to enterprise
    ELSE 0.0
  END AS monthly_revenue_opportunity,
  current_timestamp() AS scored_at
FROM STREAM(silver_customer_enriched) c
LEFT JOIN STREAM(bronze_network_slices) ns ON c.customer_id = ns.customer_id
LEFT JOIN STREAM(gold_congestion_features) cf ON ns.tower_id = cf.tower_id
WHERE c.subscription_tier IN ('BASIC', 'PREMIUM')
  AND (cf.congestion_predicted_15min = 1 OR cf.avg_congestion_score > 60);
```

```sql
-- Gold: revenue analytics aggregations
CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_analytics AS
SELECT
  DATE(scored_at) AS analysis_date,
  EXTRACT(HOUR FROM scored_at) AS hour_of_day,
  customer_segment,
  COUNT(*) AS total_opportunities,
  COUNT(CASE WHEN conversion_score > 0.7 THEN 1 END) AS high_conversion_opportunities,
  AVG(conversion_score) AS avg_conversion_score,
  SUM(monthly_revenue_opportunity) AS total_revenue_opportunity,
  SUM(CASE WHEN conversion_score > 0.7 THEN monthly_revenue_opportunity ELSE 0 END) AS high_confidence_revenue_opportunity,
  -- Simulated conversion metrics (for demo purposes)
  COUNT(CASE WHEN conversion_score > 0.7 THEN 1 END) * 0.47 AS projected_conversions,
  SUM(CASE WHEN conversion_score > 0.7 THEN monthly_revenue_opportunity ELSE 0 END) * 0.47 * 12 AS projected_annual_revenue
FROM STREAM(gold_conversion_opportunities)
GROUP BY 
  DATE(scored_at),
  EXTRACT(HOUR FROM scored_at),
  customer_segment;