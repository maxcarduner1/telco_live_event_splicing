# Project Structure

```
dynamic-network-slicing-live-events/
├── databricks.yml                                    # Asset Bundle config with dev/prod environments
├── resources/
│   ├── telemetry_pipeline.pipeline.yml              # SDP pipeline for bronze→silver→gold transformations
│   ├── nwdaf_models.model.yml                       # Congestion prediction and customer scoring models
│   ├── network_operations_dashboard.dashboard.yml   # Real-time tower health and slice provisioning dashboard
│   ├── revenue_genie_space.genie.yml               # Natural language analytics for pricing optimization
│   └── slice_automation.job.yml                     # Autonomous slice provisioning workflow
├── src/
│   ├── pipeline/
│   │   └── transformations/
│   │       ├── bronze_cell_tower_telemetry.sql      # Raw tower metrics ingestion
│   │       ├── bronze_customer_profiles.sql         # Customer and subscription data ingestion
│   │       ├── bronze_event_calendar.sql            # Major events and venue data ingestion
│   │       ├── bronze_network_slices.sql            # Active slice configurations ingestion
│   │       ├── silver_tower_metrics.sql             # Cleaned telemetry with congestion scoring
│   │       ├── silver_customer_enriched.sql         # Customer segmentation and upgrade propensity
│   │       ├── silver_event_tower_proximity.sql     # Event-tower geospatial mapping
│   │       ├── gold_congestion_features.sql         # NWDAF prediction features and trend analysis
│   │       ├── gold_conversion_opportunities.sql    # Real-time customer conversion scoring
│   │       └── gold_revenue_analytics.sql           # Revenue optimization aggregations
│   ├── models/
│   │   ├── congestion_predictor.py                  # 15-minute ahead congestion prediction model
│   │   └── customer_scorer.py                       # Premium plan conversion propensity model
│   ├── jobs/
│   │   └── autonomous_slice_provisioning.py         # Automated slice creation and SMS campaigns
│   ├── dashboards/
│   │   └── network_operations.lvdash.json           # AI/BI dashboard for real-time monitoring
│   └── data_generation/
│       ├── generate_telemetry.py                    # Synthetic cell tower metrics with event spikes
│       ├── generate_customers.py                    # Customer profiles with social influence scores
│       └── generate_events.py                       # Major events calendar with venue coordinates
├── SKILL.md                                         # Main skill definition and build instructions
├── storyline.md                                     # Business context and narrative arc
├── data-schema.md                                   # Complete table schemas and transformation SQL
├── project-structure.md                             # This file - target directory layout
└── walkthrough.md                                   # Demo script and talk track for presentations