# Process Mining on Databricks: From Event Logs to Operational Intelligence

*A few months ago, a customer asked me: "Can I do process mining on Databricks?" The short answer is yes. But exploring the question changed how I think about the relationship between data platforms and process intelligence tools — and where each one's value actually lives.*

---

## The Question

Process mining reconstructs the *actual* processes your organization runs — not the ones you designed in a Visio diagram — by analyzing event log data from operational systems. It reveals the bottlenecks, rework loops, and deviations that dashboards and reports miss.

If you're running Databricks, your operational data is likely already landing in the lakehouse. ERP transactions, CRM activities, ticketing system events — they're being ingested for BI, compliance, and analytics. So the question isn't whether you *can* do process mining. It's where the value actually lives.

---

## Start with the Partnership: Celonis + Databricks

The most mature path is the [Celonis-Databricks partnership](https://www.celonis.com/news/press/celonis-partners-with-databricks-to-power-enterprise-ai-that-continuously-improves-business-operations). Celonis connects to your lakehouse via **Delta Sharing** — zero-copy, governed, no ETL. It reads your Delta tables in place and builds its Process Intelligence Graph on top.

Celonis brings strong capabilities: pre-built connectors that know how to map SAP tables to a procure-to-pay event log, purpose-built process visualization, native AI and action flows. These tools have been built over years for exactly this problem. If your organization uses Celonis, the integration with Databricks is clean and production-grade.

But this partnership also highlights something about where Databricks fits in the stack.

---

## Where the Lakehouse Adds Value

Process mining tools — Celonis, Signavio, UiPath — are excellent at what they do: process discovery, conformance checking, visualization, and increasingly AI-powered insights. They build event logs, they analyze them, and they provide action recommendations.

What the lakehouse adds isn't a better version of what those tools do. It's **context they don't have access to**.

### The data is already here

Most Databricks customers are already ingesting their SAP, Salesforce, and ServiceNow data into the lakehouse for other purposes. Setting up a separate extraction pipeline just for process mining means duplicating work. In the lakehouse, building an event log is a transformation on data you already have — not a new pipeline.

### The event log as a governed, reusable data product

When a process mining tool builds an event log, it lives inside that tool. Other teams — BI, ML, compliance — can't easily consume it. When the event log is a Unity Catalog table, it's discoverable, governed, documented, with lineage tracked from raw source to metric. Any tool can consume it: Celonis via Delta Sharing, an AI/BI dashboard, an ML model, or pm4py for quick exploration.

### Enrichment with operational context

This is the strongest differentiator. Process mining tools' AI trains on what they see in the event log: activity sequences, timestamps, case durations. That's valuable. But the *why* behind a bottleneck often lives in other data.

Maybe a purchase order is stuck at approval because the supplier's credit rating changed last week. Maybe cases with contract amendments take 3x longer. Maybe SLA breaches correlate with specific warehouse locations during peak inventory periods. These signals live in supplier master data, contract tables, inventory systems — data that's already in the lakehouse alongside the event log.

```sql
-- Enrich process features with operational context
SELECT
    c.case_id,
    c.event_count,
    c.has_rework,
    c.case_duration_days,
    s.credit_risk_rating,
    s.on_time_delivery_rate,
    ct.amendment_count,
    ct.payment_terms_days
FROM process_mining.gold.case_summary c
JOIN procurement.supplier_master s ON c.supplier_id = s.supplier_id
JOIN procurement.contracts ct ON c.contract_id = ct.contract_id
```

This isn't about replacing what process mining tools do. It's about giving them — and your own models — richer inputs. The enrichment results can flow back to Celonis via Delta Sharing, making its AI more informed. Or they can power predictions and dashboards directly in the lakehouse.

---

## Making It Reusable: A Toolkit, Not a Demo

Talking to customers, I realized the same work gets repeated on every process mining project: figure out which tables contain events, map columns, standardize names, find enrichment sources. So we built [an open-source toolkit](https://github.com/shreya-kar/process-mining-databricks) to automate this.

### Configuration-driven event log builder

Instead of writing Spark pipelines from scratch, you define the mapping in YAML:

```yaml
event_log:
  name: procure_to_pay
  output_table: process_mining.silver.p2p_event_log
  sources:
    - table: erp.procurement.purchase_orders
      case_id: po_number
      events:
        - activity: "Create Purchase Order"
          timestamp: created_at
          resource: buyer
        - activity: "Approve Purchase Order"
          timestamp: approved_at
          condition: "approved_at IS NOT NULL"
    - table: erp.warehouse.goods_receipts
      case_id: po_number
      events:
        - activity: "Post Goods Receipt"
          timestamp: posting_date
  enrichment:
    - table: procurement.supplier_master
      join_key: supplier_id
      columns: [credit_risk_rating, on_time_delivery_rate]
```

Then build and save with three lines:

```python
from eventlog import EventLogBuilder

builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.save()
```

The framework handles extraction, standardization, event ordering, case metrics, and enrichment joins. You ship a new process type by adding a YAML file — P2P, O2C, incident management, patient journey — not by writing code.

### AI-assisted discovery

The toolkit includes a discovery agent that scans your Unity Catalog:

1. **Finds tables with event-like patterns** — timestamp columns, ID columns, status fields
2. **Uses a Foundation Model to propose the mapping** — which columns are case IDs, which timestamps represent activities, what the activities should be called
3. **Generates a YAML config** you can review, edit, and use with the builder

```python
from agents.catalog_discovery_agent import EventLogDiscoveryAgent

agent = EventLogDiscoveryAgent(spark, catalogs=["erp", "finance"])
config = agent.discover(process_hint="procure to pay")
agent.save_config(config, "my_p2p_config.yaml")
```

This is where Unity Catalog metadata + Foundation Model APIs + Spark combine in a way no other platform can: AI reads your catalog, understands your schema, and generates a working pipeline.

### Enrichment agent

Given an event log, an enrichment agent:

1. Scans the catalog for tables with joinable keys
2. Uses an LLM to assess which enrichments are business-relevant
3. Trains quick models to measure the ML impact of each enrichment
4. Ranks recommendations with evidence

Instead of guessing which data might matter, you get a scored list: "joining supplier_master on supplier_id adds +0.04 AUC to SLA breach prediction, with 92% match rate."

---

## For Teams Exploring Process Mining: The pm4py Path

Not every team has Celonis. The repo includes a **pm4py consumer** — a Databricks App with interactive process discovery, conformance checking, variant analysis, and bottleneck identification. It reads from the same silver event log the builder produces.

[pm4py](https://github.com/process-intelligence-solutions/pm4py) is the leading open-source process mining library. It handles the algorithmic work; Spark handles the data preparation. The App wraps it in an interactive dashboard using Plotly — no external dependencies.

This is a tool for exploration and proof-of-concept. It's often enough to answer: "Is process mining worth investing in for our data?"

---

## Wrapping Up

**"Can I do process mining on Databricks?"**

**Yes, with Celonis.** The partnership via Delta Sharing is the production path. It's governed, zero-copy, and Celonis brings deep process intelligence capabilities.

**Yes, with pm4py.** For exploration and POC, the open-source route works.

**But the bigger point is this:** process mining tools are great at what they do — discovery, conformance, visualization, action recommendations. The lakehouse makes them better by providing governed, enriched data. Your operational data is already in Databricks. The event log is a transformation on data you already have. And because it lives alongside supplier data, contract terms, ML models, and AI functions, you can add context that makes process intelligence more accurate and more actionable — regardless of which tool consumes it.

The toolkit is open source: [github.com/shreya-kar/process-mining-databricks](https://github.com/shreya-kar/process-mining-databricks). Fork it, add a YAML template for your process, and point the discovery agent at your catalog.

---

*The companion repo includes a config-driven event log builder, AI discovery and enrichment agents, pre-built templates (P2P, O2C, incident management), a pm4py Databricks App, AI/BI dashboard queries, and an ML prediction consumer.*
