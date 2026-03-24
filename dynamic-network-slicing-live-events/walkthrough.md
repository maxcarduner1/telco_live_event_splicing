# Demo Walkthrough: Dynamic Network Slicing for Live Events — B2B Edition

## Demo Overview

This demo showcases how TelcoMax transforms 5G network congestion from an enterprise SLA liability into a B2B revenue growth engine. The platform monitors contracted bandwidth usage for stadium business customers—broadcasters, venue operators, security teams, payment processors, and team/sponsor groups—predicts when they will exceed their allocated slice capacity, and autonomously provisions burst capacity while triggering upsell proposals to their account managers.

**KPIs Impacted:**
- **Upsell ARR**: $168K new ARR from bandwidth expansion contracts during a single event
- **Churn Prevention**: $2.4M ARR protected (accounts at risk of non-renewal after SLA degradation)
- **SLA Compliance**: Zero violations across all 35 contracted B2B accounts
- **Acceptance Rate**: 62% B2B upsell proposal acceptance during congestion events
- **Operational Efficiency**: 45-minute manual slice expansion reduced to 30 seconds

**Key Outputs:**
- Real-time network operations dashboard tracking B2B customer bandwidth utilization vs. contracted limits
- Autonomous burst-capacity provisioning for accounts approaching their ceiling
- Upsell proposal engine targeting account managers with personalized expansion offers
- Revenue optimization Genie Space for natural language analytics on contract performance
- NWDAF ML models for predictive bandwidth breach detection

## Demo Script

### Step 1: Business Context Setup (~2 min)

[Navigate to storyline.md or presentation slide showing TelcoMax overview]

"TelcoMax provides dedicated 5G network slices to 35 business customers at Lumen Field for today's USMNT vs Australia match. These are not individual consumers—these are broadcasters like ESPN and Fox Sports sending 4K feeds, Ticketmaster running gate access, Seattle PD coordinating security, and the US Soccer Federation operating coaching analytics. Each has a contracted bandwidth SLA they depend on."

[Show the five B2B customer segments on slide]

"When a soccer goal sparks 70,000 simultaneous uploads, the tower congestion doesn't just affect fan streaming—it compresses the shared radio resources that these enterprise slices run on. Without proactive action, broadcasters miss their uplink windows, payment terminals time out, and security bodycam feeds degrade. That's SLA penalties, credit requests, and churn."

### Step 2: Data Architecture Overview (~3 min)

[Navigate to Databricks workspace showing the telemetry_pipeline in Spark Declarative Pipelines]

"The foundation is real-time data from 1,200 cell towers streaming through our medallion architecture. Bronze layer ingests raw telemetry at 1-second intervals and also holds the B2B customer contract registry—35 business accounts with their contracted bandwidth allocations, SLA tiers, and renewal dates."

[Show the pipeline visualization in SDP]

"Silver layer enriches this with B2B segmentation—broadcasters, critical services, venue ops, payments, and teams—and computes an upsell propensity score for each account based on how close they are to their bandwidth ceiling, how soon their contract renews, and their recent support history."

"Gold layer creates NWDAF bandwidth breach alerts: which accounts are above 85% utilization, which towers predict congestion in the next 15 minutes, and what the upsell revenue opportunity looks like for each flagged account."

### Step 3: Live Event Simulation (~4 min)

[Navigate to the network operations dashboard showing pre-event baseline]

"Let's simulate kickoff at Lumen Field. Before the match, we see normal utilization across all 8 towers—most B2B customers running at 55–70% of their contracted bandwidth. Everything is green."

[Show the event trigger—towers starting to climb as fans arrive]

"As 70,000 fans enter and start their pre-match social media activity, tower utilization climbs. The NWDAF models detect the trend 15 minutes before congestion materially impacts slice quality."

[Navigate to dashboard showing bandwidth breach risk panel]

"Four broadcasters just crossed the 85% utilization warning threshold. ESPN is at 94% of their 500 Mbps contracted limit. Ticketmaster gate systems are at 96%. The system has flagged 12 accounts as at risk—this is the actionable intelligence Sarah's NOC would previously only have seen after a breach had already occurred."

### Step 4: Upsell Proposal and Burst Provisioning (~3 min)

[Show the gold_conversion_opportunities table results]

"The system scores each flagged account on a composite upsell score: how urgently do they need more capacity right now, how likely are they to accept an offer based on historical behavior and renewal proximity, and what's the revenue opportunity?"

[Show the breach risk levels: breach / critical / warning / watch]

"ESPN scores 0.91—critical breach risk, platinum SLA, contract renews in 4 months. The system auto-provisions a burst slice adding 250 Mbps on their serving tower in under 30 seconds, and simultaneously fires an upsell proposal to their account manager: 'ESPN is at 94% of contracted bandwidth. Expand by 250 Mbps for $2,000/month. Activate now.'"

[Navigate to the upsell proposal log]

"Watch the acceptance signals come in. Broadcasters accept at 72%—they cannot risk going black during a live match. Payment processors at 68%—a gateway timeout at halftime is catastrophic for their reputation. Eight of twelve accounts accepted capacity expansion before the 73rd minute goal."

### Step 5: Revenue Impact Visualization (~2 min)

[Navigate to the revenue optimization Genie Space]

"Let's ask our Genie Space: 'How much new ARR did we generate from the USMNT match, and how much churn risk did we prevent?'"

[Show Genie response with revenue breakdown]

"The system shows $14K in immediate monthly upsell revenue across eight accounts, projecting $168K in new ARR. But the bigger story is churn prevention: four accounts that had previously flagged non-renewal intent experienced zero degradation tonight—that's $2.4M in ARR we kept off the table."

[Navigate to the SLA compliance panel in the dashboard]

"Zero SLA violations across all 35 B2B accounts. The same match that would have generated $1.8M in SLA credits last year instead generated $168K in new revenue."

### Step 6: Natural Language Analytics (~2 min)

[Navigate to Genie Space and demonstrate various queries]

"Business users can now ask questions like: 'Which customer types have the highest upsell acceptance rates during sporting events?' or 'What's the total ARR at churn risk across our platinum broadcast accounts?'"

[Show Genie generating insights about contract renewal timing]

"The system reveals that accounts within 6 months of renewal and above 75% peak utilization accept upsell offers at 88%—the strongest signal for proactive outreach. Sarah's team can now prioritize account manager calls before the next event, not after the SLA breach."

## Executive Talk Track

### 60-Second Pitch

TelcoMax serves 35 enterprise business customers at every major live event—broadcasters, venue operators, security agencies, payment processors, and team organizations—all running on contracted 5G network slices with guaranteed bandwidth SLAs. During the USMNT vs Australia match simulation, network congestion pushed 12 of those accounts toward their contracted limits. Instead of SLA violations and churn, our AI-powered NWDAF platform detected the risk 15 minutes ahead, provisioned burst capacity in under 30 seconds, and triggered personalized upsell proposals through account managers. Result: 8 contracts expanded, $168K in new ARR, $2.4M in churn-risk ARR protected, zero SLA violations. This demonstrates how Databricks' real-time AI platform turns network congestion from a B2B liability into a contract growth engine.

### Expanded Summary

The 5G network slicing market is being won by carriers who can turn SLA guarantees from a cost center into a revenue lever. TelcoMax serves enterprise business customers at major live events through dedicated network slices with contractual bandwidth and latency guarantees. When congestion strikes—and at 70,000-fan events, it always does—those guarantees are what these customers pay for and what drives their renewal decisions.

Our solution uses Databricks' unified platform to build an AI-powered NWDAF system that monitors contracted bandwidth utilization in real time, predicts which accounts will breach their limits before they experience degradation, and autonomously acts—provisioning burst capacity and triggering account-manager upsell workflows within 30 seconds of the prediction window.

During the USMNT match simulation, the system identified 12 at-risk accounts, provisioned burst capacity for all of them, and converted 8 into expanded contracts. The financial impact: $168K in new ARR from bandwidth expansions, $2.4M in churn-risk ARR protected from non-renewal, and zero SLA violations for all 35 business accounts. Total revenue impact per event: $2.6M versus a historical $1.8M in losses.

## Architecture Flow

```
Cell Tower Telemetry (1,200 towers, 1-sec intervals)
    ↓
B2B Customer Contracts + Event Calendar + Network Slices
    ↓
Bronze Layer (Raw Ingestion via Spark Declarative Pipelines)
    ↓
Silver Layer (B2B Segmentation + Upsell Propensity Scoring)
    ↓
Gold Layer (NWDAF Bandwidth Breach Alerts + Upsell Scoring)
    ↓
ML Models (Congestion Prediction + Customer Upsell Scoring via Model Serving)
    ↓
┌─ Network Operations Dashboard (B2B Utilization vs. Contracted Bandwidth)
├─ Revenue Genie Space (NL Analytics: ARR at risk, upsell pipeline, churn)
└─ Autonomous Jobs (Burst Provisioning + Upsell Proposal Delivery)
    ↓
Business Outcomes (Upsell ARR + Churn Prevention + SLA Compliance)
```

## Audience Adaptations

### C-Suite

**Focus**: "This transforms your biggest operational risk—enterprise SLA violations—into your largest upsell opportunity. Every time the network gets stressed during a major event, you currently absorb $1.8M in credits and non-renewals. This platform flips that into $2.6M of value per event: new contract ARR and protected renewals. Scale this across your full event calendar, and you're talking about tens of millions in incremental annual revenue while eliminating SLA exposure."

**Key Metrics**: $2.6M event revenue swing, 62% B2B upsell acceptance, $0 SLA credits vs. $1.8M historical baseline.

### Technical Leadership

**Focus**: "The architecture handles real-time slice utilization tracking at telecom scale—35 enterprise accounts, 8 serving towers, 1-second telemetry intervals—through Spark Declarative Pipelines' medallion architecture. The new `utilization_pct` metric on bronze_network_slices is the key signal: it's the ratio of current to contracted bandwidth, computed per slice and streamed into the gold breach-detection layer. Burst slices write back into bronze_network_slices transactionally via Jobs, closing the loop without any external integration."

**Key Capabilities**: Real-time utilization monitoring, B2B propensity scoring, streaming breach detection, automated burst provisioning.

### Individual Contributors

**Focus**: "The implementation adds `contracted_bandwidth_mbps` and `current_bandwidth_mbps` to the bronze slice schema, enabling the silver layer to compute utilization ratios inline. Gold layer breach alerting uses a composite score: upsell propensity (from silver enrichment) weighted 45%, utilization pressure weighted 35%, and congestion prediction bonus 20%. The autonomous job writes burst slices back into bronze, which the SDP pipeline re-reads on its next micro-batch—closed-loop actuation in pure SQL."

**Key Patterns**: Schema evolution via auto-merge, utilization-ratio streaming aggregations, multi-signal upsell scoring, B2B-aware burst provisioning.
