# Agentic Event Log Generator for Databricks

An AI-powered toolkit that discovers, builds, and enriches process mining event logs from any tables in Unity Catalog — using agentic skills on Claude Code or Genie Code.

**Point an agent at your catalog. It scans, reasons, tests, and builds a governed, enriched event log. Consume it with Celonis via Delta Sharing, or explore with the pm4py Databricks App.**

Companion blog: [Process Mining on Databricks: From Event Logs to Operational Intelligence](blog.md)

## Why This Exists

Every process mining project starts with the same bottleneck: building a clean event log from messy operational tables. Which tables have events? What's the case ID? How do you standardize activity names across source systems? Where's the reference data to enrich with?

This toolkit automates that with an agentic skill that:
- **Scans** your catalog and profiles tables using UC metadata
- **Reasons** about which tables are event sources vs reference data vs aggregates
- **Tests** every mapping with real SQL before committing
- **Enriches** with operational context (supplier data, contracts, account info)
- **Validates** quality gates and self-corrects if something fails

The result is a governed event log in Unity Catalog — ready for Celonis, pm4py, or any tool.

## Architecture

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                         Unity Catalog                            │
 │                                                                  │
 │  Source Tables ──▶ Agent Skill ──▶ Enriched Event Log            │
 │  (ERP, CRM,       (scans, reasons,  (governed UC table)          │
 │   ITSM, etc.)      tests, builds)         │                     │
 │                                           ├──▶ Celonis           │
 │  Runs on:                                 │    (Delta Sharing)   │
 │  • Claude Code (.claude/skills/)          │                      │
 │  • Genie Code  (.assistant/skills/)       └──▶ pm4py App         │
 │                                                (Databricks App)  │
 └──────────────────────────────────────────────────────────────────┘
```

---

## Setup

### One-Command Setup

```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./setup.sh
```

The setup script checks for (and helps install) all prerequisites:
- Claude Code
- Databricks CLI + workspace authentication
- Databricks AI Dev Kit (MCP tools)

### Manual Setup

<details>
<summary>Click to expand</summary>

**1. Install Claude Code**
```bash
npm install -g @anthropic-ai/claude-code
```

**2. Install Databricks CLI + authenticate**
```bash
brew tap databricks/tap && brew install databricks
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
```

Make sure `[DEFAULT]` in `~/.databrickscfg` points to your workspace.

**3. Install Databricks AI Dev Kit** (provides MCP tools)
```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
```

Restart Claude Code after installing.

**4. Clone and go**
```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
claude
```

</details>

---

## Quick Start

### Option A: Claude Code (recommended)

From this repo directory:

```
/discover-event-log
```

> "Build an event log from dbdemos.sales_pipeline — it's a sales pipeline process"

Claude scans your catalog using Databricks MCP tools, reasons about mappings, tests with real SQL, and produces an enriched event log in Unity Catalog.

### Option B: Genie Code (no Claude Code or Anthropic subscription needed)

Copy the skill to your workspace:

```bash
databricks workspace import-dir ./genie-code/discover-event-log \
  /Workspace/Users/<you>/.assistant/skills/discover-event-log
```

In Genie Code (Agent mode):

> @discover-event-log Build an event log from dbdemos.sales_pipeline

Same skill, same logic. Genie Code has native UC metadata access and runs on Databricks FMAPI.

### Option C: Use a YAML template (no AI needed)

```python
%pip install /Workspace/Users/<you>/agentic-databricks-event-log-generator/

from eventlog import EventLogBuilder
builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.save()
```

---

## Consuming the Event Log

Once the agent builds the event log, consume it with:

### Celonis (via Delta Sharing)

Run `governance/setup_sharing.py` to create a Delta Share. Celonis connects via the sharing protocol — zero-copy, always current, governed.

### pm4py Databricks App

Deploy the interactive process mining dashboard:

```bash
cd consumers/pm4py-app
databricks apps create process-mining-dashboard --app-source .
```

Shows process maps (DFG), variant analysis, bottleneck transitions, conformance checking — all reading from the event log table.

---

## Repo Structure

```
agentic-databricks-event-log-generator/
│
├── .claude/skills/discover-event-log/  # Claude Code skill
│   ├── SKILL.md                        #   4-phase agentic workflow
│   └── references/                     #   YAML schema + process patterns
│
├── genie-code/discover-event-log/      # Genie Code skill (same logic)
│   └── SKILL.md
│
├── eventlog/                           # Python package (pip-installable)
│   ├── builder.py                      #   YAML config → Spark pipeline → event log
│   ├── enricher.py                     #   Catalog scanner for enrichment discovery
│   ├── validator.py                    #   Event log quality checks
│   └── schemas.py                      #   Config validation
│
├── templates/                          # Pre-built YAML configs
│   ├── procure_to_pay.yaml
│   ├── order_to_cash.yaml
│   └── incident_management.yaml
│
├── consumers/
│   └── pm4py-app/                      #   Databricks App (Dash + pm4py + Plotly)
│
├── governance/
│   └── setup_sharing.py                #   Delta Sharing to Celonis etc.
│
├── examples/demo_p2p/                  #   Synthetic data for testing
├── notebooks/02_build_event_log.py     #   Standalone builder notebook
├── setup.py                            #   pip install
├── setup.sh                            #   One-command setup
├── CLAUDE.md
├── blog.md
└── README.md
```

## How the Skill Works

The skill teaches the agent process mining domain knowledge. The agent uses its built-in capabilities (MCP tools for Claude Code, native UC access for Genie Code) for table profiling.

**Phase 1 — Discover:** Profile tables. Classify as event source (timestamps + high-cardinality ID), reference data (IDs, no timestamps), or aggregate (skip).

**Phase 2 — Map & Test:** Identify case ID, map timestamp columns to activities, handle snapshot tables by deriving intermediate stages. Test every extraction with real SQL.

**Phase 3 — Build & Validate:** UNION ALL extractions + enrichment joins + window functions. Quality gates: no nulls, >=2 activities, >1 event per case. Self-correct if gates fail.

**Phase 4 — Report:** Save to Unity Catalog, print summary.

## Tested On

| Dataset | Source | Result |
|---|---|---|
| `dbdemos.sales_pipeline` | Salesforce-style CRM (snapshot) | 17,734 events, 3,019 cases, 7 activities, enriched with accounts + reps |
| `process_mining.erp_raw` | Simulated SAP P2P (multi-table) | 27,647 events, 5,000 cases, 6 activities, enriched with suppliers + contracts |
| `dbdemos.lakehouse_iot` | IoT sensor data | Correctly identified as NOT suitable for process mining (no process stages) |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `/discover-event-log` not recognized | Make sure you're in the repo directory |
| MCP tool errors | AI Dev Kit not installed. Run setup.sh |
| Auth errors | Set `[DEFAULT]` in `~/.databrickscfg` |
| pm4py app empty | Grant app service principal `SELECT` on event log table |

## License

Apache 2.0
