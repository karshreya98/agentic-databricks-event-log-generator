# Agentic Event Log Generator for Databricks

## What This Repo Is

Toolkit for discovering, building, and enriching process mining event logs on Databricks using agentic skills. Produces governed UC tables consumable by Celonis (Delta Sharing) or pm4py (Databricks App).

## Key Components

- `.claude/skills/discover-event-log/` — Agentic skill: scan → reason → test → build
- `genie-code/discover-event-log/` — Same skill for Genie Code
- `eventlog/` — Python package: EventLogBuilder, EventLogEnricher, EventLogValidator
- `templates/` — YAML configs for P2P, O2C, incident management
- `consumers/pm4py-app/` — Interactive process mining dashboard
- `governance/` — Delta Sharing setup

## Skills

- `/discover-event-log` — Agentic event log discovery using Databricks MCP tools

## Working with Databricks

- `get_table_details` for table profiling (schema + samples + stats in one call)
- `execute_sql` for testing mappings and building tables
- `manage_uc_objects` for catalog/schema creation
- `manage_uc_tags` for reading PII/business tags
