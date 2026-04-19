# Agentic Event Log Generator for Databricks

An AI-powered toolkit that discovers, builds, and enriches process mining event logs from any tables in Unity Catalog.

**Point an agent at your catalog. It scans, reasons, tests, and builds a governed, enriched event log.**

Companion blog: [Process Mining on Databricks: From Event Logs to Operational Intelligence](blog.md)

## Why Databricks

Your operational data (SAP, Salesforce, ServiceNow) is already in the lakehouse. Process mining needs the same data — just shaped differently. This toolkit automates that shaping.

- **Unity Catalog metadata** — the agent reads table schemas, column stats, descriptions, and tags to understand your data. No manual profiling.
- **Enrichment beyond the event log** — supplier risk ratings, contract terms, customer data sit alongside the event log. Process mining tools don't have access to this context. The lakehouse does.
- **Event log as a governed data product** — not locked in a tool. Celonis reads it via Delta Sharing. The pm4py app visualizes it. ML models train on it. Same table, same governance.

This complements tools like Celonis — it automates the hardest part (building the event log) so they can do what they're best at (analysis).

---

## How It Works

```
You: "Build an event log from my_catalog.erp_schema — it's a procure-to-pay process"

Agent:
  Phase 1 — DISCOVER    Profiles tables using UC metadata.
                         Classifies as event source / reference / aggregate.
                         Detects existing event logs (skips building if found).

  Phase 2 — MAP & TEST  Identifies case ID, maps timestamps to activities.
                         Tests every extraction with real SQL.
                         Handles snapshot tables (derives intermediate stages).

  Phase 3 — BUILD       UNION ALL + enrichment joins + window functions.
                         Validates quality gates. Self-corrects if gates fail.
                         Supports both traditional (single case_id) and
                         OCEL 2.0 (multi-object) output.

  Phase 4 — REPORT      Saves enriched event log to Unity Catalog.
```

The skill teaches the agent **process mining domain knowledge** — table classification, event mapping, snapshot handling, enrichment patterns, OCEL output. The agent uses its built-in capabilities for profiling and SQL execution.

### Output Formats

**Traditional:** Flat event log with single `case_id`. Ready for Celonis, pm4py, or any standard PM tool.

**OCEL 2.0:** Three tables (events + objects + event-to-object links). Preserves many-to-many relationships (e.g., one PO → multiple invoices → multiple suppliers). Required for object-centric process mining.

### Column Taxonomy (Celonis-aligned)

| Category | Examples |
|----------|---------|
| **Mandatory three** | case_id, activity, event_timestamp |
| **Activity-level** | resource, department, source_system, cost, rework flag |
| **Case-level (enrichments)** | supplier_name, credit_risk_rating, contract_type, region |
| **Computed** | event_rank, time_since_prev_seconds, case_event_count |

---

## Getting Started

### Option A: Claude Code

Best quality reasoning. Requires Anthropic subscription + Databricks AI Dev Kit.

```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./scripts/setup-claude-code.sh    # installs Claude Code + Databricks CLI + AI Dev Kit
claude                             # start Claude Code from repo directory
/discover-event-log                # invoke the skill
```

> "Build an event log from my_catalog.my_schema — it's a procure-to-pay process. Include OCEL output."

### Option B: Genie Code

No external dependencies. Runs inside Databricks on FMAPI.

```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./scripts/install-genie-code.sh    # copies skill to your workspace
```

Open Genie Code (Agent mode):

> @discover-event-log Build an event log from my_catalog.my_schema — it's a sales pipeline process

Same skill, same logic, same output. See [`scripts/install-genie-code.sh`](scripts/install-genie-code.sh) for profile options.

---

## Examples

The `examples/` folder provides an end-to-end walkthrough:

| File | What it does |
|------|-------------|
| `01_create_source_tables.py` | Databricks notebook — generates realistic P2P tables with many-to-many relationships and data quality noise (duplicates, nulls, orphans, typos) |
| `02_run_discovery_claude_code.md` | Step-by-step guide: run the skill with Claude Code, compare traditional vs OCEL |
| `03_run_discovery_genie_code.md` | Same guide for Genie Code |

The synthetic data includes:
- 5 operational tables (PRs → POs → GRs → Invoices → Payments) with realistic 1:N relationships
- 3 reference tables (suppliers, contracts, cost centers)
- Injected noise: 3% duplicate events, 2% null timestamps, orphan records, supplier ID typos, out-of-order timestamps

---

## Consuming the Event Log

**Celonis / Signavio (Delta Sharing):**
```sql
CREATE SHARE process_mining_share;
ALTER SHARE process_mining_share ADD TABLE my_catalog.silver.event_log;
```

**pm4py Databricks App:**
```bash
cd app
# Edit app.yaml: set DATABRICKS_WAREHOUSE_ID, CATALOG, SCHEMA
databricks apps create process-mining-dashboard --app-source .
```

The app auto-discovers all event log tables in the configured catalog/schema. Supports both traditional and OCEL tables — OCEL tables show object type breakdown and links-per-event stats alongside the standard process map.

---

## Repo Structure

```
agentic-databricks-event-log-generator/
│
├── skills/                             # Agentic skill (single source of truth)
│   └── discover-event-log/
│       ├── SKILL.md                    #   4-phase agentic workflow + OCEL support
│       └── references/                 #   YAML schema + process patterns
│
├── .claude/skills/                     # Claude Code auto-detection (symlink → skills/)
│
├── scripts/
│   ├── setup-claude-code.sh            #   One-command Claude Code setup
│   └── install-genie-code.sh           #   One-command Genie Code install
│
├── app/                                # pm4py Databricks App
│   ├── app.py                          #   Multi-table dashboard with OCEL support
│   ├── app.yaml
│   └── requirements.txt
│
├── examples/                           # End-to-end walkthrough
│   ├── 01_create_source_tables.py      #   Synthetic P2P data with noise
│   ├── 02_run_discovery_claude_code.md
│   └── 03_run_discovery_genie_code.md
│
├── CLAUDE.md
├── blog.md
└── README.md
```

## What the Skill Handles

| Data shape | What happens |
|---|---|
| Multi-table ERP (separate PO, GR, Invoice, Payment tables) | Discovers mappings, builds event log, enriches |
| Snapshot table (one row per entity with stage column) | Derives intermediate stages from timestamps |
| Existing event log (has case_id/activity/timestamp) | Skips building, validates + enriches |
| Many-to-many relationships (PO → N invoices) | Generates OCEL output preserving all object links |
| Not process data (IoT, time series) | Rejects with explanation |
| Noisy data (duplicates, nulls, orphans) | Quality gates catch issues, reports them |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `/discover-event-log` not recognized | Run Claude Code from the repo directory |
| MCP tool errors | Run `./scripts/setup-claude-code.sh` |
| Genie Code doesn't see skill | Run `./scripts/install-genie-code.sh`, refresh page |
| pm4py app empty | Grant app service principal `SELECT` on the event log table and `CAN_USE` on the warehouse |
| OCEL table errors in app | Make sure `_ocel_events`, `_ocel_objects`, `_ocel_e2o` tables all exist with matching prefixes |

## License

Apache 2.0
