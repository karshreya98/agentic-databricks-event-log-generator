# Genie Code Skills for Process Mining

Use these skills with [Genie Code](https://www.databricks.com/product/genie-code) — Databricks' autonomous AI agent. No Claude Code or Anthropic subscription needed.

## Setup

Copy the skill folder to your workspace:

```bash
# For all users in the workspace
databricks workspace mkdirs /Workspace/.assistant/skills/discover-event-log
databricks workspace import-dir ./genie-code/discover-event-log /Workspace/.assistant/skills/discover-event-log

# Or for just your user
databricks workspace mkdirs /Workspace/Users/<you>/.assistant/skills/discover-event-log
databricks workspace import-dir ./genie-code/discover-event-log /Workspace/Users/<you>/.assistant/skills/discover-event-log
```

## Usage

In Genie Code (Agent mode), use `@discover-event-log` or just describe what you want:

> "Build me a process event log from the sales_pipeline schema in dbdemos"

Genie Code will load the skill and follow the 4-phase workflow:
1. **Scan** — profiles tables using UC metadata
2. **Reason** — classifies as event source / reference / aggregate
3. **Test** — validates mappings with real SQL
4. **Build** — creates the event log, checks quality, saves to UC

## Difference from Claude Code Version

| | Claude Code | Genie Code |
|---|---|---|
| Skill format | Same (SKILL.md) | Same (SKILL.md) |
| Location | `.claude/skills/` | `.assistant/skills/` |
| Invoke | `/discover-event-log` | `@discover-event-log` |
| UC metadata | Via MCP tools (get_table_details) | Native (built-in) |
| Model | Anthropic API | Databricks FMAPI |
| Install | Claude Code + AI Dev Kit | Nothing extra |

The skill logic is identical. Only the runtime and metadata access differ.
