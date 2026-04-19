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

The skill teaches the agent **process mining domain knowledge** — table classification, event mapping, snapshot handling, enrichment patterns. The agent uses its built-in capabilities for the actual profiling and SQL execution.

## Two Ways to Run

### Claude Code

Best quality. Requires Anthropic subscription + Databricks AI Dev Kit.

**Setup:**
```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./setup.sh    # checks/installs Claude Code, Databricks CLI, AI Dev Kit
```

**Run:**
```bash
claude                        # start Claude Code from repo directory
/discover-event-log           # invoke the skill
```
> "Build an event log from my_catalog.my_schema — it's a procure-to-pay process"

The skill lives in `.claude/skills/discover-event-log/SKILL.md` and is auto-detected when Claude Code runs in this directory.

### Genie Code

No external dependencies. Runs inside Databricks on FMAPI.

**Setup:**
```bash
git clone https://github.com/karshreya98/agentic-databricks-event-log-generator.git
cd agentic-databricks-event-log-generator
./install-genie-code.sh                        # copies skill to your workspace
# or with a specific profile:
./install-genie-code.sh --profile my-workspace
```

**Run:**

Open Genie Code in your workspace (Agent mode):

> @discover-event-log Build an event log from my_catalog.my_schema — it's a sales pipeline process

Same skill logic, same output. Genie Code has native UC metadata access — no MCP tools needed.

### Without AI (YAML templates)

For users who already know their table mappings:

```python
%pip install /Workspace/Users/<you>/agentic-databricks-event-log-generator/

from eventlog import EventLogBuilder
builder = EventLogBuilder(spark, "templates/procure_to_pay.yaml")
builder.save()
```

Edit `templates/procure_to_pay.yaml` to match your tables. See `.claude/skills/discover-event-log/references/yaml-schema.md` for the config format.

---

## Consuming the Event Log

Once built, the event log is a governed Unity Catalog table. Consume it with:

**Celonis / Signavio (Delta Sharing):**
```sql
CREATE SHARE IF NOT EXISTS process_mining_share;
ALTER SHARE process_mining_share ADD TABLE process_mining.silver.event_log;
-- Grant to recipient, they connect via Delta Sharing protocol
```

**pm4py Databricks App:**
```bash
cd consumers/pm4py-app
databricks apps create process-mining-dashboard --app-source .
```
Interactive process maps, variant analysis, bottleneck transitions, conformance checking.

---

## Repo Structure

```
agentic-databricks-event-log-generator/
│
├── .claude/skills/discover-event-log/  # Claude Code skill
│   ├── SKILL.md                        #   Agentic workflow (4 phases)
│   └── references/                     #   YAML schema + process patterns
│
├── genie-code/discover-event-log/      # Genie Code skill (same logic)
│   └── SKILL.md
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
├── consumers/pm4py-app/                # Databricks App (Dash + pm4py)
│
├── setup.sh                            # Claude Code setup (one command)
├── install-genie-code.sh               # Genie Code install (one command)
├── setup.py                            # pip install for eventlog package
├── CLAUDE.md
├── blog.md
└── README.md
```

## Tested Results

| Dataset | Type | Result |
|---|---|---|
| `dbdemos.sales_pipeline` | CRM snapshot (Salesforce-style) | 17,734 events, 3,019 cases, 7 activities, enriched with accounts + reps |
| `process_mining.erp_raw` | Multi-table ERP (SAP-style) | 27,647 events, 5,000 cases, 6 activities, enriched with suppliers + contracts |
| `dbdemos.lakehouse_iot` | IoT sensor data | Correctly identified as NOT suitable for process mining |

Both Claude Code and Genie Code produced **identical results** on the same dataset.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `/discover-event-log` not recognized | Run Claude Code from the repo directory |
| MCP tool errors | Run `./setup.sh` to install AI Dev Kit |
| Auth errors | `databricks auth login`, set `[DEFAULT]` in `~/.databrickscfg` |
| Genie Code doesn't see skill | Run `./install-genie-code.sh`, refresh Genie Code |
| pm4py app empty | Grant app service principal `SELECT` on event log table |

## License

Apache 2.0
