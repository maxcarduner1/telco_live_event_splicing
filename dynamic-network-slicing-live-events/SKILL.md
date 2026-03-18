---
name: dynamic-network-slicing-live-events
description: "Builds an AI-powered 5G network slicing platform that predicts congestion 15 minutes ahead and autonomously provisions premium slices during live events, converting network stress into revenue opportunities for telecom operators."
---

# Dynamic Network Slicing for Live Events

## Overview
This demo builds an AI-powered network slicing platform for TelcoMax, a Tier-1 wireless carrier serving 45M subscribers. The system predicts network congestion 15 minutes before it occurs during major events and autonomously provisions premium 5G slices, converting network stress into revenue by offering instant premium plan upgrades to high-value customers attempting to stream or go live.

## Before You Start
- Read [storyline.md](storyline.md) for business context and narrative arc
- Read [data-schema.md](data-schema.md) for all table schemas and transformation SQL
- Read [project-structure.md](project-structure.md) for target directory layout
- Read [walkthrough.md](walkthrough.md) for demo walkthrough script and talk track

## Prerequisites
- Unity Catalog with `telcomax` catalog and `network_ops` schema
- Serverless compute enabled
- Model Serving endpoints available
- Streaming table capabilities

## Build Steps
1. **Generate synthetic data** — Read the `databricks-synthetic-data-generation` skill and create the telemetry and customer datasets defined in data-schema.md
2. **Build streaming pipelines** — Read the `databricks-spark-declarative-pipelines` skill and implement the bronze→silver→gold medallion architecture from data-schema.md
3. **Deploy NWDAF models** — Read the `databricks-model-serving` skill and deploy the congestion prediction and customer scoring models
4. **Create operations dashboard** — Read the `databricks-aibi-dashboards` skill and build the real-time network monitoring dashboard
5. **Build revenue analytics** — Read the `databricks-genie` skill and create the natural language query space for pricing optimization
6. **Deploy automation workflows** — Read the `databricks-jobs` skill and create the autonomous slice provisioning jobs

## Acceptance Criteria
- [ ] Real-time telemetry streams through medallion architecture with <30 second latency
- [ ] ML models predict congestion events 15+ minutes ahead with >85% accuracy
- [ ] Dashboard shows live tower health, congestion predictions, and active slice provisioning
- [ ] Genie Space answers natural language questions about revenue optimization and customer behavior
- [ ] Automated workflows provision premium slices and trigger upgrade offers during simulated events
- [ ] Demo shows $6.2M ARR generation from single event simulation with 47% conversion rate