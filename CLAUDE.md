# Agentic Event Log Generator for Databricks

## What This Repo Is

Toolkit for discovering, building, and enriching process mining event logs on Databricks. Uses agentic skills (Claude Code or Genie Code) to scan catalogs, reason about table structure, and produce governed event logs.

## Skills

- `/discover-event-log` — Agentic event log discovery. Scans catalog, classifies tables, maps events, tests with SQL, enriches, validates quality.

## Key Components

- `.claude/skills/discover-event-log/` — Claude Code skill (auto-detected in this directory)
- `genie-code/discover-event-log/` — Same skill for Genie Code
- `eventlog/` — Python package: EventLogBuilder, EventLogEnricher, EventLogValidator
- `templates/` — YAML configs for P2P, O2C, incident management
- `consumers/pm4py-app/` — Interactive process mining Databricks App

## Databricks MCP Tools Used

- `get_table_details` — profiles tables (schema + samples + stats in one call)
- `execute_sql` — tests mappings, validates quality, builds tables
- `manage_uc_objects` — creates catalogs/schemas
- `manage_uc_tags` — reads PII/business tags
