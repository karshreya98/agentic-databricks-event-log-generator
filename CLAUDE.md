# Process Mining Toolkit for Databricks

## What This Repo Is

A toolkit for building, enriching, and consuming process event logs on Databricks.
Config-driven event log builder + agentic discovery skill + pluggable consumers.

## Key Components

- `eventlog/` — Python package: EventLogBuilder, EventLogEnricher, EventLogValidator
- `templates/` — YAML configs for P2P, O2C, incident management
- `consumers/` — pm4py app, AI/BI dashboard, ML predictions
- `governance/` — Delta Sharing, table docs, PII tags
- `notebooks/02_build_event_log.py` — Standalone builder notebook

## Skills

- `/discover-event-log` — Agentic event log discovery using Databricks MCP tools. Scans catalog, samples data, reasons about mappings, tests with SQL, validates quality.

## Working with Databricks

- Use `execute_sql` MCP tool for SQL queries against the warehouse
- Use `manage_uc_objects` for catalog/schema/volume operations
- Use `run_python_file_on_databricks` for running Python on clusters
- Demo tables live in the `process_mining` catalog on e2-demo-field-eng

## YAML Config Format

Event log configs follow the schema in `.claude/skills/discover-event-log/references/yaml-schema.md`
