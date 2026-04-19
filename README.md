# Agentic Event Log Generator for Databricks

An AI-powered toolkit that discovers, builds, and enriches process mining event logs from any tables in Unity Catalog.

**Point an agent at your catalog. It scans, reasons, tests, and builds a governed, enriched event log.**

Companion blog: [Process Mining on Databricks: From Event Logs to Operational Intelligence](blog.md)

## How It Works

```
You: "Build an event log from dbdemos.sales_pipeline — it's a sales pipeline process"

Agent:
  Phase 1 — DISCOVER    Profiles tables using UC metadata.
                         Classifies as event source / reference / aggregate.

  Phase 2 — MAP & TEST  Identifies case ID, maps timestamps to activities.
                         Tests every extraction with real SQL.

  Phase 3 — BUILD       UNION ALL + enrichment joins + window functions.
                         Validates quality gates. Self-corrects if gates fail.

  Phase 4 — REPORT      Saves enriched event log to Unity Catalog.
                         17,734 events | 3,019 cases | 7 activities | 19 columns
```

The skill teaches the agent **process mining domain knowledge**. The agent uses its built-in capabilities for profiling and SQL execution.

---

## Getting Started

### Claude Code

```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./scripts/setup-claude-code.sh    # installs Claude Code + Databricks CLI + AI Dev Kit
claude                             # start Claude Code
/discover-event-log                # invoke the skill
```

> "Build an event log from my_catalog.my_schema — it's a procure-to-pay process"

### Genie Code

```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./scripts/install-genie-code.sh    # copies skill to your workspace
```

Open Genie Code (Agent mode):

> @discover-event-log Build an event log from my_catalog.my_schema — it's a sales pipeline process

### Without AI

```python
%pip install /Workspace/Users/<you>/agentic-databricks-event-log-generator/

from eventlog import EventLogBuilder
builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.save()
```

---

## Examples

End-to-end walkthrough in `examples/`:

| File | What it does |
|------|-------------|
| `01_create_source_tables.py` | Databricks notebook — generates realistic ERP tables to discover |
| `02_run_discovery_claude_code.md` | Step-by-step guide for Claude Code |
| `03_run_discovery_genie_code.md` | Step-by-step guide for Genie Code |

---

## Consuming the Event Log

**Celonis / Signavio (Delta Sharing):**
```sql
CREATE SHARE process_mining_share;
ALTER SHARE process_mining_share ADD TABLE process_mining.silver.event_log;
```

**pm4py Databricks App:**
```bash
cd app
databricks apps create process-mining-dashboard --app-source .
```

---

## Repo Structure

```
agentic-databricks-event-log-generator/
│
├── skills/                             # Agentic skill (single source of truth)
│   └── discover-event-log/
│       ├── SKILL.md                    #   4-phase workflow
│       └── references/                 #   YAML schema + process patterns
│
├── .claude/skills/                     # Claude Code auto-detection (symlink → skills/)
│
├── scripts/
│   ├── setup-claude-code.sh            #   Install Claude Code + AI Dev Kit
│   └── install-genie-code.sh           #   Copy skill to Databricks workspace
│
├── app/                                # pm4py Databricks App
│   ├── app.py
│   ├── app.yaml
│   └── requirements.txt
│
├── eventlog/                           # Python package (pip-installable)
│   ├── builder.py                      #   YAML config → Spark → event log
│   ├── enricher.py                     #   Catalog scanner for enrichments
│   ├── validator.py                    #   Quality checks
│   └── schemas.py                      #   Config validation
│
├── templates/                          # Pre-built YAML configs
│   ├── procure_to_pay.yaml
│   ├── order_to_cash.yaml
│   └── incident_management.yaml
│
├── examples/                           # End-to-end walkthrough
│   ├── 01_create_source_tables.py
│   ├── 02_run_discovery_claude_code.md
│   └── 03_run_discovery_genie_code.md
│
├── setup.py
├── CLAUDE.md
├── blog.md
└── README.md
```

## Tested Results

| Dataset | Type | Result |
|---|---|---|
| `dbdemos.sales_pipeline` | CRM snapshot | 17,734 events, 3,019 cases, 7 activities |
| `process_mining.erp_raw` | Multi-table ERP | 27,647 events, 5,000 cases, 6 activities |
| `dbdemos.lakehouse_iot` | IoT sensor data | Correctly rejected — not process data |

Claude Code and Genie Code produced **identical results** on the same dataset.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `/discover-event-log` not recognized | Run Claude Code from the repo directory |
| MCP tool errors | Run `./scripts/setup-claude-code.sh` |
| Genie Code doesn't see skill | Run `./scripts/install-genie-code.sh`, refresh |
| pm4py app empty | Grant app service principal `SELECT` on the table |

## License

Apache 2.0
