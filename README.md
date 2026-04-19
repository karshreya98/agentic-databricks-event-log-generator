# Process Mining Toolkit for Databricks

A reusable, open-source toolkit for building, enriching, and consuming process event logs on the Databricks Lakehouse.

**Discover event logs with AI. Build from config. Enrich with operational context. Consume with any tool.**

Companion blog: [Process Mining on Databricks: From Event Logs to Operational Intelligence](blog.md)

## Why This Exists

Every process mining project repeats the same work: figure out which tables contain events, map columns, standardize names, find enrichment data. This toolkit automates that.

It's not a replacement for process mining tools like Celonis — those are excellent at discovery, conformance, and visualization. This toolkit focuses on what the lakehouse is uniquely good at: **building governed, enriched event logs from messy operational data at scale**, and making them consumable by any tool.

## Architecture

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                         Unity Catalog                            │
 │                                                                  │
 │  Source Tables ──▶ eventlog/ ──▶ Silver Event Log ──▶ Consumers  │
 │  (ERP, CRM,       (builder +     (governed,          │          │
 │   ITSM, etc.)      enricher)      enriched)           │          │
 │                        ▲                               ├─ Celonis │
 │                        │                               ├─ pm4py   │
 │               /discover-event-log                      ├─ AI/BI   │
 │               (Claude Code skill:                      └─ ML      │
 │                scans, samples, reasons,                           │
 │                tests, validates)                                  │
 └──────────────────────────────────────────────────────────────────┘
```

---

## Setup

### One-Command Setup

```bash
git clone https://github.com/shreya-kar/process-mining-databricks.git
cd process-mining-databricks
./setup.sh
```

The setup script checks for (and helps install) all prerequisites:
- Claude Code
- Databricks CLI + workspace authentication
- Databricks AI Dev Kit (MCP tools)

### Manual Setup (if you prefer)

<details>
<summary>Click to expand step-by-step instructions</summary>

**1. Install Claude Code**
```bash
npm install -g @anthropic-ai/claude-code
```

**2. Install Databricks CLI**
```bash
brew tap databricks/tap && brew install databricks
```

**3. Authenticate to your workspace**
```bash
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
```

Make sure `[DEFAULT]` in `~/.databrickscfg` points to your workspace — the MCP tools read this to connect.

**4. Install Databricks AI Dev Kit** (provides the MCP tools: `execute_sql`, `get_table_details`, etc.)
```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
```

Restart Claude Code after installing.

**5. Clone and go**
```bash
git clone https://github.com/shreya-kar/process-mining-databricks.git
cd process-mining-databricks
claude
```

</details>

### For Cluster-Based Usage Only (no Claude Code needed)

If you just want the `eventlog` Python package and templates — no agentic discovery:

```python
# On a Databricks notebook
%pip install /Workspace/Users/<you>/process-mining-databricks/
```

---

## Quick Start

### Option A: Agentic Discovery with Claude Code (recommended)

From this repo directory, start Claude Code and run:

```
/discover-event-log
```

Then tell it what you're looking for:

> "Build an event log from the erp catalog — it's a procure-to-pay process"

Claude scans your catalog using Databricks MCP tools (`get_table_details`, `execute_sql`, `manage_uc_tags`), reasons about mappings, tests extractions with real SQL, validates quality, and produces a tested YAML config + materialized event log.

**What happens under the hood:**

```
You ──▶ Claude Code ──▶ MCP tools ──▶ Databricks workspace
                            │
         get_table_details ─┤── profiles tables (schema, samples, stats)
         manage_uc_tags ────┤── reads PII/business tags
         execute_sql ───────┤── tests mappings, validates, builds tables
         manage_uc_objects ─┘── creates catalogs/schemas
```

### Option B: Genie Code (no Claude Code or Anthropic subscription needed)

Same agentic skill, running inside [Genie Code](https://www.databricks.com/product/genie-code) — Databricks' autonomous AI agent. Uses FMAPI, not Anthropic API.

```bash
# Copy skill to your workspace
databricks workspace import-dir ./genie-code/discover-event-log /Workspace/.assistant/skills/discover-event-log
```

Then in Genie Code (Agent mode):

> @discover-event-log "Build an event log from dbdemos.sales_pipeline"

Same 4-phase workflow (scan → reason → test → build). Genie Code has native access to UC metadata, so no MCP tools needed.

See [`genie-code/README.md`](genie-code/README.md) for details.

### Option C: Use a Pre-Built Template (no AI needed)

Edit a YAML template to match your table/column names, then build:

```python
from eventlog import EventLogBuilder

builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.build()       # preview
builder.summary()     # stats
builder.save()        # write to Unity Catalog
```

### Option D: Build + Validate in a Notebook

Import `notebooks/02_build_event_log.py` into your workspace — reads a YAML config, builds the event log, and validates quality using the `eventlog` package.

### Option E: API Usage in Your Own Code

```python
from eventlog import EventLogBuilder, EventLogEnricher, EventLogValidator

# Build from config
builder = EventLogBuilder(spark, "my_config.yaml")
builder.save()

# Discover enrichments from the catalog
enricher = EventLogEnricher(spark, builder.build())
candidates = enricher.discover(catalogs=["procurement"])
enriched = enricher.apply(candidates)

# Validate
EventLogValidator(enriched).report()
```

### Generate Demo Data

Run the notebooks in `examples/demo_p2p/` to create synthetic P2P data and see the pipeline end-to-end.

---

## Repo Structure

```
process-mining-databricks/
│
├── .claude/skills/discover-event-log/  # Agentic discovery (Claude Code skill)
│   ├── SKILL.md                        #   4-phase workflow: scan → reason → test → validate
│   └── references/                     #   YAML schema + process pattern guides
│
├── eventlog/                           # Python package (pip-installable)
│   ├── builder.py                      #   YAML config → Spark pipeline → event log
│   ├── enricher.py                     #   Catalog scanner for enrichment discovery
│   ├── validator.py                    #   Event log quality checks
│   └── schemas.py                      #   Config validation
│
├── templates/                          # Pre-built process configs
│   ├── procure_to_pay.yaml             #   P2P
│   ├── order_to_cash.yaml              #   O2C
│   └── incident_management.yaml        #   ITSM
│
├── genie-code/                         # Databricks-native path (no Claude Code needed)
│   ├── discover-event-log/SKILL.md     #   Same skill, runs in Genie Code
│   └── README.md                       #   Setup instructions
│
├── consumers/                          # Pluggable consumers of the event log
│   ├── pm4py-app/                      #   Interactive PM dashboard (Databricks App)
│   ├── aibi-dashboard/                 #   Executive KPIs (Lakeview queries)
│   └── ml-predictions/                 #   SLA breach prediction (AutoML)
│
├── notebooks/
│   └── 02_build_event_log.py           #   Standalone builder notebook
│
├── governance/
│   └── setup_sharing.py                #   Delta Sharing, table docs, PII tags
│
├── examples/demo_p2p/                  #   Synthetic P2P data for testing
│
├── setup.py                            #   pip install for eventlog package
├── CLAUDE.md                           #   Repo instructions for Claude Code
├── blog.md                             #   Companion blog post
└── README.md
```

## Components

### `/discover-event-log` — Agentic Event Log Discovery

A Claude Code skill that uses Databricks MCP tools iteratively:

1. **Scan** — Profiles all tables in a schema with `get_table_details` (schema, row counts, sample values, unique counts, value distributions — all in one call)
2. **Reason** — Classifies tables as event source / reference / aggregate based on metadata. Identifies case IDs from cardinality patterns.
3. **Test** — Validates every mapping with real SQL via `execute_sql` before committing
4. **Build** — Creates the event log with enrichments, checks quality gates, self-corrects if validation fails

Unlike a single LLM call, the agent reads actual data, catches column mismatches, handles join conflicts, and iterates until the event log passes quality checks.

### `eventlog/` — Config-Driven Framework

```python
builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.save()
# ==================================================
#   Events:            27,647
#   Cases:              5,000
#   Activities:             6
#   Source tables:           4
# ==================================================
```

**`enricher.py`** scans Unity Catalog for joinable reference tables:

```python
enricher = EventLogEnricher(spark, event_log_df)
candidates = enricher.discover(catalogs=["procurement"])
enriched_df = enricher.apply(candidates)
```

**`validator.py`** checks event log quality (nulls, duplicates, timestamp ordering, activity cardinality):

```python
EventLogValidator(event_log_df).report()
```

### `templates/` — Community Process Configs

| Template | Process | Key Activities |
|----------|---------|---------------|
| `procure_to_pay.yaml` | P2P | PR → PO → GR → Invoice → Payment |
| `order_to_cash.yaml` | O2C | Order → Credit → Pick/Pack/Ship → Invoice → Payment |
| `incident_management.yaml` | ITSM | Create → Assign → Investigate → Resolve → Close |

**Contributing a template:** Add a YAML file following the schema in `.claude/skills/discover-event-log/references/yaml-schema.md`.

### `consumers/` — Pluggable Event Log Consumers

Once you have an event log table, consume it with any of these:

- **`pm4py-app/`** — Databricks App: interactive process maps, variants, bottlenecks, conformance (Dash + pm4py + Plotly)
- **`aibi-dashboard/`** — SQL queries for a Lakeview executive dashboard
- **`ml-predictions/`** — SLA breach predictor comparing process-only vs. enriched features (AutoML)
- **`governance/setup_sharing.py`** — Delta Share the event log to Celonis or other external tools

## Scaling

- **Builder (Spark):** Billions of raw events, distributed.
- **Enricher (Spark):** Catalog scanning and joins are distributed.
- **pm4py (pandas):** Single-node — filter or sample at Spark level before `.toPandas()`.
- **Delta Sharing:** Zero-copy to external tools.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `/discover-event-log` not recognized | Make sure you're in the repo directory when running Claude Code |
| "unknown tool" errors on MCP calls | AI Dev Kit not installed. Run the install script from Step 4 and restart Claude Code |
| Auth errors from MCP tools | `[DEFAULT]` profile in `~/.databrickscfg` is empty or points to wrong workspace. Run `databricks auth login` |
| `eventlog` package import errors on cluster | Upload the repo to workspace and `%pip install` from the workspace path |
| pm4py app shows empty graphs | Check that the app's service principal has `SELECT` on the event log table |

## License

Apache 2.0
