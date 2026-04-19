# Genie Code Setup

Use the `discover-event-log` skill with Genie Code — no Claude Code or Anthropic subscription needed.

## Install

From the repo root:

```bash
./install-genie-code.sh
# or with a specific Databricks profile:
./install-genie-code.sh --profile my-workspace
```

This copies the skill to `/Workspace/Users/<you>/.assistant/skills/discover-event-log/`.

## Use

In Genie Code (Agent mode):

> @discover-event-log Build an event log from dbdemos.sales_pipeline — it's a sales pipeline process

## How It Differs from Claude Code

| | Claude Code | Genie Code |
|---|---|---|
| Skill format | SKILL.md | SKILL.md (identical) |
| Location | `.claude/skills/` (local) | `.assistant/skills/` (workspace) |
| Invoke | `/discover-event-log` | `@discover-event-log` |
| UC metadata | Via MCP tools | Native |
| Model | Anthropic API | Databricks FMAPI |
| Install | Claude Code + AI Dev Kit | `./install-genie-code.sh` |

Same skill logic, same output.
