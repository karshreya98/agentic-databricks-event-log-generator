# Process Mining on Databricks: From Event Logs to Operational Intelligence

*A few months ago, a customer asked me: "Can I do process mining on Databricks?" The answer led me to build an open-source toolkit that bridges the gap between the operational data already in your lakehouse and the process mining tools that analyze it.*

---

## The Starting Point: Your Data Is Already Here

This is the insight that matters most. If you're running Databricks, your operational data — SAP transactions, Salesforce opportunities, ServiceNow tickets, ERP purchase orders — is already landing in the lakehouse. It's being ingested for BI, compliance, analytics, ML. It's governed in Unity Catalog with lineage, access controls, and semantic metadata.

Process mining needs exactly this data. Not a copy of it in another tool. Not a separate extraction pipeline. The same tables, governed the same way, enriched with the same operational context that already lives alongside them.

The question was never "can Databricks do process mining." It was "why would you build the event log anywhere else when the data, the governance, and the context are already here?"

---

## Enterprise Process Mining Tools Are Great — And They Agree

Enterprise process mining tools — Celonis, SAP Signavio, UiPath Process Mining — are excellent at what they do: process discovery, conformance checking, visualization, action recommendations, and increasingly their own AI capabilities. They've invested years in these features and they do them well.

Celonis has a [strategic partnership with Databricks](https://www.celonis.com/news/press/celonis-partners-with-databricks-to-power-enterprise-ai-that-continuously-improves-business-operations) built on Delta Sharing. Signavio and UiPath support similar integrations. These tools *want* to read from a governed data platform rather than maintain their own extraction pipelines. The industry is converging on the idea that the data platform is the foundation and the process mining tool is the analytical layer on top.

The partnership model works because both sides do what they're best at:
- **Databricks** → data engineering, governance, enrichment, serving
- **Process mining tools** → discovery, conformance, visualization, action

---

## What the Lakehouse Uniquely Brings

### Unity Catalog as the Semantic Layer

This is underappreciated. Unity Catalog doesn't just store tables — it stores *knowledge about* tables. Column descriptions, table comments, data types, tags (PII, business-critical), lineage from source to consumption, access controls.

When an AI agent profiles your catalog to build an event log, it reads all of this. A column tagged `business_key=true` is probably a case ID. A table with a comment saying "supplier master data" is enrichment material, not an event source. A column with 6 unique values named `stagename` with a progression from "Discovery" to "Closed Won" is a process stage.

This semantic layer means the agent understands your data *before it reads a single row*. No process mining tool has access to this depth of metadata about your source systems.

### Enrichment: The Story Beyond the Event Log

Every process mining tool — Celonis included — has AI and ML capabilities that operate on event log features: activity sequences, timestamps, case durations, resources. These are valuable.

But the *why* behind a bottleneck usually lives outside the event log:
- A purchase order stuck at approval → the supplier's credit rating changed last week (supplier master data)
- Cases with contract amendments take 3x longer → the contract terms changed mid-process (contract data)
- SLA breaches cluster in Q4 → warehouse capacity was at 98% (inventory data)
- Invoice rejections spike for one vendor → three-way match failures correlate with a specific product category (quality data)

These signals sit in the lakehouse *alongside* the event log. The event log is the process backbone; the rest of the catalog is the operational context. Together, they tell a story that no single tool sees on its own.

Our toolkit enriches the event log at build time — joining supplier risk ratings, contract terms, cost center data, customer segments — so that whether Celonis reads it via Delta Sharing or you explore it with pm4py, the context is already there.

### The Event Log as a Governed Data Product

When a process mining tool builds an event log internally, it lives inside that tool. The BI team can't query it. The ML team can't train on it. The compliance team can't audit it. It's a tool artifact, not a data product.

When the event log is a Unity Catalog table:
- It has lineage (you can trace from the event log back to the raw SAP table it came from)
- It has governance (the same access controls as everything else in your catalog)
- It has discoverability (anyone can find it, understand its schema, see its description)
- It has multiple consumers (Celonis via Delta Sharing, pm4py app, AI/BI dashboards, ML models — all from the same table)

One event log, many consumers, one governance model.

---

## Automating the Hard Part with AI Agents

Building the event log is the bottleneck. Which tables contain events? What's the case ID? How do you handle snapshot tables? Where's the enrichment data? This takes weeks of manual data engineering on every project.

We built an [open-source toolkit](https://github.com/karshreya98/agentic-databricks-event-log-generator) that automates this with an agentic AI skill. The skill teaches an agent process mining domain knowledge — table classification, event mapping, enrichment patterns, quality validation. The agent uses its built-in capabilities to scan your catalog and execute.

It runs on two runtimes:
- **Claude Code** — uses the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) MCP tools (`get_table_details`, `execute_sql`, `manage_uc_tags`) to profile tables, test mappings, and build the event log. The AI Dev Kit is an open-source collection of MCP tools and skills that connects Claude Code to Databricks workspaces.
- **Genie Code** — runs inside the Databricks workspace with native UC access. No external dependencies or subscriptions needed.

Same skill, same output, your choice of runtime.

### What the agent does

```
Phase 1 — DISCOVER    Profiles tables using UC metadata (schemas, stats,
                       descriptions, tags). Classifies as event source,
                       reference data, aggregate, or existing event log.

Phase 2 — MAP & TEST  Identifies case ID. Maps timestamps to activities.
                       Tests every extraction with real SQL. Handles
                       snapshot tables by deriving intermediate stages.

Phase 3 — BUILD       Combines extractions + enrichment joins.
                       Validates quality gates. Self-corrects on failure.
                       Outputs traditional (single case_id) or OCEL 2.0
                       (multi-object) format.

Phase 4 — REPORT      Saves enriched event log to Unity Catalog.
                       Columns follow the Celonis taxonomy: mandatory three
                       (case_id, activity, timestamp) + activity-level
                       attributes (resource, department, cost) + case-level
                       enrichments (supplier, contract, region).
```

### Traditional vs Object-Centric (OCEL)

Traditional process mining forces a single case ID per event. Real processes have many-to-many relationships: one PO → multiple invoices, one invoice → multiple POs.

OCEL 2.0 preserves all object relationships:

```
Traditional:  "Create PO" belongs to case_id = PO-001

OCEL:         "Create PO" links to PurchaseOrder: PO-001
                                    Supplier: SUP-042
                                    Contract: CTR-010
```

The toolkit generates traditional output by default. If the agent detects multiple object types per event, it asks at a checkpoint whether to also build OCEL — opt-in, not automatic. Both Celonis and pm4py support OCEL on the consumption side.

---

## Consuming the Event Log

### Enterprise Tools (Delta Sharing)

The event log is a governed UC table. Share it to Celonis, Signavio, or UiPath via Delta Sharing — zero-copy, always current:

```sql
CREATE SHARE process_mining_share;
ALTER SHARE process_mining_share ADD TABLE my_catalog.silver.event_log;
GRANT SELECT ON SHARE process_mining_share TO RECIPIENT celonis_recipient;
```

The recipient configures the connection on their side:
- [Databricks: Create and manage shares](https://docs.databricks.com/en/delta-sharing/create-share.html)
- [Celonis: Connect to Databricks](https://docs.celonis.com/en/databricks.html)
- [SAP Signavio: External data sources](https://help.sap.com/docs/signavio)
- [UiPath Process Mining: Databricks connector](https://docs.uipath.com/process-mining)

### Exploration (pm4py Databricks App)

For teams exploring process mining without an enterprise tool, the toolkit includes a Databricks App built with [pm4py](https://github.com/process-intelligence-solutions/pm4py) — the leading open-source process mining library. It provides interactive process maps, variant analysis, bottleneck detection, and conformance checking. The app auto-discovers all event log tables in the catalog and supports both traditional and OCEL formats.

**Scope:** the app is a reference implementation suited for small-to-medium event logs (≲ 200K events). For production-scale workloads, share the event log to an enterprise process mining tool via Delta Sharing — those tools are built for it.

**Note on pm4py licensing:** pm4py is licensed under AGPL-3.0, which requires derivative works to be open-sourced. This is fine for internal exploration, POCs, and research. For commercial or production use, evaluate the licensing implications or use an enterprise tool like Celonis instead.

---

## Wrapping Up

**"Can I do process mining on Databricks?"**

Yes. And the bigger question is: why would you build the event log anywhere else?

Your operational data is already in the lakehouse. Unity Catalog already has the semantic metadata to understand it. The enrichment data — supplier ratings, contract terms, customer segments — already sits alongside the event log. The governance model already covers it.

Process mining tools are great at analysis. The lakehouse is great at being the governed, enriched, central data platform they read from. This toolkit automates the bridge between the two — turning raw operational tables into a governed event log that any tool can consume.

The toolkit is open source: [github.com/karshreya98/agentic-databricks-event-log-generator](https://github.com/karshreya98/agentic-databricks-event-log-generator).
