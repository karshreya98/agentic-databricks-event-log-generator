# Agentic Event Log Generator for Databricks

## Skills

- `/discover-event-log` — Agentic event log discovery. Auto-detected via `.claude/skills/` symlink → `skills/discover-event-log/`.

## Repo Layout

- `skills/` — Canonical skill definition (shared by Claude Code + Genie Code)
- `eventlog/` — Python package: EventLogBuilder, EventLogEnricher, EventLogValidator
- `templates/` — YAML configs for P2P, O2C, incident management
- `app/` — pm4py Databricks App
- `examples/` — Synthetic data + step-by-step guides
- `scripts/` — Setup and install scripts

## Databricks MCP Tools Used by the Skill

- `get_table_details` — profiles tables (schema + samples + stats in one call)
- `execute_sql` — tests mappings, validates quality, builds tables
- `manage_uc_objects` — creates catalogs/schemas
- `manage_uc_tags` — reads PII/business tags
