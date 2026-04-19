# Process Mining on Databricks: From Event Logs to Operational Intelligence

*A few months ago, a customer asked me: "Can I do process mining on Databricks?" That question led to an open-source toolkit that uses AI agents to automate the hardest part of process mining — building the event log.*

---

## The Question

Process mining reconstructs the *actual* processes your organization runs by analyzing event log data from operational systems. It reveals bottlenecks, rework loops, and deviations that dashboards miss.

If you're running Databricks, your operational data is likely already in the lakehouse. SAP transactions, Salesforce activities, ServiceNow tickets — ingested for BI, compliance, and analytics. Process mining needs the same data, just shaped differently. It doesn't require a new pipeline. It requires a new lens.

But every process mining project hits the same bottleneck: **building a clean, enriched event log from raw operational tables**. Which tables contain events? What's the case ID? How do you handle snapshot tables that aren't event histories? Where's the reference data to enrich with? This takes days to weeks of manual data engineering.

---

## Start with the Partnership: Celonis + Databricks

The most mature production path is the [Celonis-Databricks partnership](https://www.celonis.com/news/press/celonis-partners-with-databricks-to-power-enterprise-ai-that-continuously-improves-business-operations). Celonis connects to your lakehouse via **Delta Sharing** — zero-copy, governed, no ETL.

Celonis brings pre-built connectors for SAP, purpose-built visualization, native AI, and action flows. If your organization uses Celonis, the Databricks integration is production-grade.

To set up Delta Sharing with Celonis, see [Celonis Data Integration: Databricks](https://docs.celonis.com/en/databricks.html). Signavio and UiPath Process Mining also support Delta Sharing — check their respective documentation for connector setup:
- [SAP Signavio: External Data Integration](https://help.sap.com/docs/signavio)
- [UiPath Process Mining: Data Connectors](https://docs.uipath.com/process-mining)

---

## Where the Lakehouse Adds Value

Process mining tools are excellent at analysis. What the lakehouse adds is **context they don't have access to**.

**The data is already here.** Most Databricks customers already ingest their ERP/CRM/ITSM data. Building an event log is a transformation on existing data, not a new pipeline.

**Enrichment beyond the event log.** A PO stuck at approval might have nothing to do with the process — maybe the supplier's credit rating changed. That signal lives in the lakehouse alongside the event log, not inside the process mining tool.

**The event log as a governed data product.** When Celonis builds an event log, it lives in Celonis. Here, the event log is a Unity Catalog table — governed, discoverable, consumable by any tool: Celonis via Delta Sharing, pm4py for exploration, ML models for prediction.

---

## Automating Event Log Discovery with AI Agents

The [companion toolkit](https://github.com/karshreya98/agentic-databricks-event-log-generator) uses an agentic skill to automate event log creation. The skill teaches an AI agent process mining domain knowledge — table classification, event mapping, snapshot handling, enrichment patterns. The agent uses its built-in capabilities to execute.

It runs on two runtimes:

**Claude Code** — best reasoning quality. The agent uses Databricks MCP tools (`get_table_details`, `execute_sql`) to profile tables, test mappings, and build the event log.

**Genie Code** — runs inside the Databricks workspace with native UC access. No external dependencies or subscriptions needed.

Same skill, same logic, same output. One SKILL.md file works in both.

### What the agent does

```
Phase 1 — DISCOVER    Profiles tables. Classifies as event source,
                       reference data, aggregate, or existing event log.

Phase 2 — MAP & TEST  Identifies case ID. Maps timestamps to activities.
                       Tests every extraction with real SQL.
                       Handles snapshot tables (derives intermediate stages).

Phase 3 — BUILD       Combines extractions + enrichment joins.
                       Validates quality gates (no nulls, >2 activities,
                       >1 event per case). Self-corrects if gates fail.
                       Supports traditional and OCEL 2.0 output.

Phase 4 — REPORT      Saves to Unity Catalog. Reports table, event,
                       case, and enrichment statistics.
```

### What it handles

| Data shape | What happens |
|---|---|
| Multi-table ERP (POs, invoices, GRs, payments) | Discovers mappings, builds event log, enriches |
| Snapshot table (one row per entity with stage column) | Derives intermediate stages proportionally |
| Existing event log | Skips building, validates + enriches |
| Many-to-many relationships | Generates OCEL 2.0 output (events + objects + E2O) |
| Not process data (IoT, time series) | Rejects with explanation |
| Noisy data (duplicates, nulls, orphans) | Quality gates catch and report issues |

### Traditional vs OCEL output

Traditional process mining forces a single `case_id` per event. But real processes have many-to-many relationships: one PO can have multiple invoices, one invoice can cover multiple POs.

**OCEL 2.0** (Object-Centric Event Log) preserves all relationships:

```
Traditional:  "Create PO" → case_id = PO-001 (one perspective)

OCEL:         "Create PO" → PurchaseOrder: PO-001
                           → Supplier: SUP-042
                           → Contract: CTR-010  (three perspectives)
```

The toolkit generates both formats. Celonis and pm4py both support OCEL.

---

## Consuming the Event Log

### Celonis / Signavio / UiPath (Delta Sharing)

The event log is a Unity Catalog table. Share it to external process mining tools via Delta Sharing:

```sql
CREATE SHARE process_mining_share;
ALTER SHARE process_mining_share ADD TABLE my_catalog.silver.event_log;
GRANT SELECT ON SHARE process_mining_share TO RECIPIENT celonis_recipient;
```

The recipient configures the Delta Sharing connection on their side. Data stays in the lakehouse — zero-copy, always current, governed.

**Setup guides:**
- [Databricks: Create and manage shares](https://docs.databricks.com/en/delta-sharing/create-share.html)
- [Celonis: Connect to Databricks via Delta Sharing](https://docs.celonis.com/en/databricks.html)
- [SAP Signavio: External data sources](https://help.sap.com/docs/signavio)
- [UiPath Process Mining: Databricks connector](https://docs.uipath.com/process-mining)

### pm4py Databricks App (for teams without Celonis)

The repo includes an interactive Databricks App built with Dash + pm4py + Plotly. It auto-discovers all event log tables in the configured catalog and supports both traditional and OCEL formats.

```bash
cd app
databricks apps create process-mining-dashboard --app-source .
```

Shows process maps (DFG), variant analysis, bottleneck transitions, conformance checking. For OCEL tables, it additionally shows object type breakdown and links-per-event distribution.

This is a tool for exploration and proof-of-concept — often enough to answer: "Is process mining worth investing in for our data?"

---

## Wrapping Up

**"Can I do process mining on Databricks?"**

**Yes, with Celonis.** Delta Sharing is the production path. The event log lives in Unity Catalog; Celonis reads it zero-copy.

**Yes, with pm4py.** For exploration and POC, the Databricks App provides interactive process mining without additional tooling.

**Yes, and you can automate the hard part.** The agentic skill scans your catalog, reasons about your tables, builds an enriched event log, and saves it as a governed data product — in minutes, not weeks.

The toolkit is open source: [github.com/karshreya98/agentic-databricks-event-log-generator](https://github.com/karshreya98/agentic-databricks-event-log-generator).
