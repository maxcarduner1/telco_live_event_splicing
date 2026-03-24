# Storyline

## Industry Context

The global 5G network slicing market is projected to reach $13.9B by 2027, driven by enterprise demand for guaranteed Quality of Experience (QoE) and Service Level Agreements (SLAs). Tier-1 carriers like Verizon, AT&T, and T-Mobile are racing to monetize their 5G investments through dynamic network slicing—the ability to create isolated virtual networks with dedicated bandwidth, latency, and reliability guarantees.

However, most carriers still operate reactive network management systems that can't handle the explosive traffic spikes during major events. A single soccer match with 70,000 fans can generate 400% traffic increases within a 2-mile radius, simultaneously putting pressure on the dedicated slices contracted by stadium business customers—broadcasters, venue operators, security teams, payment processors, and team/sponsor organizations.

The opportunity lies in transforming network congestion from an SLA-violation risk into a B2B upsell engine. By predicting when contracted business customers will exceed their bandwidth allocations and proactively offering capacity expansions, carriers can convert network stress into incremental contract revenue while preventing churn of their most valuable enterprise accounts.

## Company Persona

**TelcoMax** is a Tier-1 wireless carrier serving 45 million subscribers across North America, with $47B in annual revenue and a premium enterprise customer base that includes 2.3M business accounts. The company operates 12,000 cell towers and has invested $8.2B in 5G infrastructure over the past three years.

**Sarah Kim, VP of Network Operations**, leads a 340-person team responsible for maintaining 99.9% uptime SLAs for enterprise customers. Sarah's biggest challenge is the reactive nature of traditional network management—by the time a contracted business customer's slice reaches saturation, their service has already degraded, triggering SLA penalties, credit requests, and contract non-renewals.

Sarah's team currently uses legacy OSS/BSS systems that provide network visibility but lack predictive capabilities. During a recent Champions League qualifying match at Lumen Field, her team manually escalated emergency capacity after detecting congestion, but three broadcast partners had already experienced uplink degradation during the critical second half, resulting in $1.8M in SLA credits and two contract non-renewals worth $2.4M in ARR.

Sarah needs an AI-powered platform that can predict congestion 15 minutes before it impacts contracted business customers, autonomously expand slice capacity or trigger account-team upsell workflows, and turn potential service failures into revenue growth.

## Business Problem

TelcoMax faces three critical challenges during major live events:

1. **Reactive Capacity Management**: Current systems detect slice saturation after business customer experience has already degraded, making it impossible to prevent SLA violations before they trigger credits and churn.

2. **Missed Upsell Windows**: When broadcasters, venue operators, and payment processors are pushing against their contracted bandwidth limits, they are most willing to purchase capacity expansions—but TelcoMax lacks the ability to identify and act on these opportunities in real time.

3. **Manual Slice Provisioning**: Expanding a contracted network slice currently requires 45–60 minutes of manual configuration by network engineers, far too slow to respond to dynamic demand during a live match.

The financial impact is significant: TelcoMax loses approximately $22M annually from congestion-related SLA credits, B2B contract non-renewals, and missed upsell opportunities. A single major event can generate $2M+ in losses within a 4-hour window.

## Customer Groups at the Stadium

**Broadcasters and media rights holders** – Buy dedicated uplink slices for 4K/8K video contribution, multi-angle replay feeds, and reporter live shots. Their bandwidth requirements surge during key moments (goals, halftime) and they cannot tolerate any degradation during on-air windows.

**Stadium / venue operator** – Contracts slices to guarantee connectivity for building operations: access-control turnstiles, CCTV, IoT sensors, digital signage, and staff communications across the bowl, concourses, and perimeter.

**Security and public safety** – Police, private security, medical, and emergency services require priority or isolated slices for bodycams, push-to-talk, crowd-analytics cameras, and emergency coordination. Reliability is non-negotiable.

**Ticketing and payments** – Merchandise vendors, food & beverage POS systems, and mobile-ticketing kiosks contract low-latency, high-reliability slices so turnstiles and card payments never fail during peak load.

**Teams, leagues, and sponsors** – Use slices for AR/VR fan experiences, live stats apps, coaching video, and performance analytics that require consistent throughput and low latency throughout the match.

## Narrative Arc

### Act 1: The Problem (Current State)
The demo begins during a simulated USMNT vs Australia match at Lumen Field in Seattle. As 70,000 fans enter the stadium, network traffic begins climbing toward dangerous levels across all eight towers serving the venue. Contracted business customers—seven broadcasters, the venue operator, security teams, fifteen payment processors, and five team/sponsor groups—are all drawing on their allocated slices.

### Act 2: The Prediction (AI Intervention)
Fifteen minutes before kickoff, the NWDAF models detect subtle patterns in cell tower telemetry that predict imminent congestion on the Lumen Field towers. The system cross-references contracted bandwidth allocations and identifies twelve business customers whose current utilization trajectories will breach 90% of their contracted limits within the prediction window. Three broadcasters and two payment processors are flagged as high-risk: they will hit 100% utilization precisely when they need it most.

### Act 3: The Upsell (Revenue Generation)
As congestion materializes, the system autonomously provisions burst capacity for the highest-risk accounts and triggers targeted upsell proposals to their account managers and procurement portals: "Lumen Field towers are approaching capacity limits. Expand your bandwidth allocation by 200 Mbps for $1,800/month—activate in under 30 seconds." Five of twelve targeted accounts accept the expansion, and three more convert from standard to gold SLA tiers.

### Act 4: The Outcome (Business Impact)
By the end of the match, TelcoMax has upsold eight business contracts, generating $14K in immediate monthly revenue and $168K in new annual recurring revenue. More importantly, zero SLA violations occurred, $2.4M in at-risk ARR was protected from churn, and two accounts that had previously signaled non-renewal intent converted to multi-year agreements after experiencing proactive service.

## Wow Moment

The climactic moment occurs at the 73rd minute when the USMNT scores the winning goal. As 70,000 fans simultaneously attempt to share the moment on social media while every broadcaster cuts to live coverage, network traffic spikes to 847% of baseline capacity. Instead of broadcast uplinks degrading on air, the dashboard shows:

- **12 business customers** proactively flagged before reaching their contracted limits
- **8 upsell contracts** accepted for capacity expansion or tier upgrade
- **$168K new ARR** generated from bandwidth expansion contracts
- **$2.4M ARR protected** from churn prevention (accounts that would have non-renewed)
- **Zero SLA violations** across all contracted business customers
- **30-second provisioning** for all burst capacity requests

The system has transformed what would have been TelcoMax's worst enterprise SLA event into their most profitable match—proving that AI-powered network slicing can turn B2B congestion risk into contract growth.

## Domain Terminology

**5G Network Slicing**: Virtual partitioning of a single physical 5G network into multiple logical networks, each with dedicated resources and performance characteristics.

**NWDAF (Network Data Analytics Function)**: AI/ML functions defined in 5G standards that provide predictive analytics and automated decision-making for network operations.

**QoE (Quality of Experience)**: End-user perception of service quality, measured through metrics like latency, throughput, and connection reliability.

**SLA (Service Level Agreement)**: Contractual commitments to specific performance levels, typically 99.9% uptime and guaranteed bandwidth for enterprise business customers.

**OSS/BSS (Operations Support Systems/Business Support Systems)**: Backend systems that manage network operations and customer billing/provisioning.

**Cell Tower Telemetry**: Real-time performance data from cellular base stations, including signal strength, bandwidth utilization, connection counts, and error rates.

**Contracted Bandwidth**: The guaranteed Mbps allocation a business customer has purchased as part of their SLA. Utilization exceeding this threshold triggers SLA review and potential penalties.

**Bandwidth Utilization Ratio**: Current throughput divided by contracted bandwidth. Values above 0.85 flag a customer as at risk; values above 1.0 indicate an active SLA breach.

**Upsell Opportunity**: A business customer approaching or exceeding their contracted bandwidth limit who represents a prime target for capacity expansion or tier upgrade offers.

**Dynamic Slice Provisioning**: Automated creation and configuration of network slices in response to predicted demand, without manual intervention.

**Churn Prevention Value**: The annual contract value protected by proactively resolving a business customer's service degradation before it triggers a non-renewal decision.

**B2B Revenue Per Congestion Event**: Key metric measuring the incremental ARR generated from capacity upsells plus the ARR protected from churn during network stress events.
