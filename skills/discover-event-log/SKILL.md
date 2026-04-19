---
name: discover-event-log
description: Agentic event log discovery for process mining. Classifies tables as event sources or reference data, maps columns to process events, builds enriched event logs, and validates quality. Use when someone says "discover event log", "build event log from", "process mining on", "find events in", or wants to create a process event log from existing tables.
user-invocable: true
---

# Agentic Event Log Discovery

Build a process mining event log from tables in Unity Catalog.

**Announce at start:** "I'm using the discover-event-log skill to build a process event log from your catalog data."

## Prerequisites Check (run FIRST)

Verify MCP tools are available by calling `get_best_warehouse()`.

- **Works** → continue
- **"unknown tool"** → AI Dev Kit not installed. Tell user: `bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)` then restart Claude Code
- **Auth error** → `databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com` and set `[DEFAULT]` in `~/.databrickscfg`

## What This Skill Adds (on top of MCP tools)

The AI Dev Kit gives you `get_table_details`, `execute_sql`, `manage_uc_objects`, `manage_uc_tags`. This skill adds **process mining domain knowledge**:

- How to classify tables for process mining (event source vs reference vs aggregate)
- How to map columns to event log fields (case_id, activity, timestamp)
- How to handle snapshot tables that aren't event histories
- How to validate an event log for process mining readiness
- How to find and apply enrichments that add operational context
- Common process patterns (P2P, O2C, sales pipeline, ITSM)

## Input

1. **Catalogs/schemas** — where the source data lives
2. **Process hint** — what kind of process
3. **Output table** — where to write (default: `process_mining.silver.event_log`)

---

## Phase 1: Discover & Classify

### Profile tables

Use `get_table_details(catalog, schema, table_stat_level="SIMPLE")` to profile all tables in one call. This returns per table: row count, column types, sample values, unique counts, value distributions, table comments, and DDL.

Optionally use `manage_uc_tags(action="query_column_tags", catalog_filter=...)` for PII/business tags.

### Classification Rules

| Classification | What to look for in the metadata | Action |
|---|---|---|
| **Existing event log** | Has columns named `case_id`/`activity`/`timestamp` (or similar), multiple rows per case ID, high row count | **Skip building — go straight to enrichment + validation** |
| **Event source** | Has date/timestamp columns + a high-cardinality ID (`unique_count` near `total_rows`) | Extract events |
| **Reference data** | Has ID columns matching event tables, no timestamps. `comment` describes it as master/reference. | Enrichment |
| **Aggregate** | Very low `total_rows`. Columns are dimensions with low `unique_count`. No high-cardinality ID. | **Skip** |
| **Duplicate** | Same columns as another table, similar `total_rows`. Name prefix `raw_`/`cleaned_`. | **Skip** |

### If an Event Log Already Exists

A table is likely already an event log if it has:
- A column like `case_id`, `process_id`, or `trace_id` (high cardinality)
- A column like `activity`, `event`, `action`, or `step` (low-to-moderate cardinality with descriptive string values)
- A timestamp column like `event_timestamp`, `timestamp`, `event_time`
- Multiple rows per case ID (avg rows per case_id > 1)

If you find one: **don't rebuild it.** Instead:
1. Validate it (Phase 3 quality gates)
2. Look for enrichment tables to join (Phase 2.4)
3. Add window functions (event_rank, time_since_prev) if missing
4. Save the enriched version to the output table

### Key Column Identification

- **case_id**: High `unique_count` near `total_rows`, appears across tables. Examples: `po_number`, `opportunityid`.
- **Timestamps**: `data_type` is date/timestamp. Each becomes a potential event.
- **Status/stage**: Low `unique_count` with `value_counts` showing a progression → snapshot table.
- **Resource**: Moderate cardinality (user IDs, names).

### Snapshot Tables (critical pattern)

Tables with one row per entity + a `stage`/`status` column showing CURRENT state. Example: `opportunity` with `createddate`, `closedate`, `stagename`.

Handle by: creating an event per timestamp column, deriving intermediate stages proportionally. **Always note this as an approximation.**

---

## Phase 2: Map & Test

For each event source, define: activity name, timestamp column, optional condition/resource/cost.

**Test every mapping with `execute_sql`** — run the extraction and verify timestamps look reasonable, case_id is populated, row count makes sense.

For enrichments: test join match rate with `execute_sql`. Handle column name mismatches (`accountid` vs `id`) and conflicts (alias duplicates like `account_name`, `rep_name`).

---

## Phase 3: Build & Validate

Build with `execute_sql`:

```sql
CREATE OR REPLACE TABLE <output_table> AS
SELECT
  case_id, activity, event_timestamp, resource, cost, source_table,
  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) AS event_rank,
  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(
    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
  ) AS time_since_prev_seconds,
  COUNT(*) OVER (PARTITION BY case_id) AS case_event_count
FROM (<union_with_enrichment_joins>)
```

Use `manage_uc_objects` to create catalog/schema if needed.

### Quality Gates

| Check | Pass |
|---|---|
| Total events | > 0 |
| Null case_ids / timestamps | = 0 |
| Distinct activities | >= 2 |
| Avg events per case | > 1 |

**If any fails → back to Phase 2.**

---

## Phase 4: Report

```
Event Log Discovery Complete
================================
Process:      <name>
Output:       <table>
Events / Cases / Activities: <counts>
Enrichments:  <tables + match rates>
Quality:      All gates passed
Tables used:  <n>
Tables skipped: <n> (with reasons)
```

---

## Error Recovery

| Error | Recovery |
|---|---|
| Join key mismatch | Check reference table's primary key, use explicit ON clause |
| Duplicate column after join | Alias enrichment columns |
| Empty extraction | Add IS NOT NULL condition on timestamp |
| Aggregate misidentified | Low row count + no unique ID → reclassify and skip |
| Snapshot table | Derive events from timestamps, note approximation |

---

## Process Mining Patterns Reference

**Procure-to-Pay:** case_id = PO number. PR → Approve → PO → GR → Invoice → Clear → Payment. Enrich: supplier_master, contracts.

**Order-to-Cash:** case_id = order ID. Order → Credit → Pick/Pack/Ship → Invoice → Payment. Enrich: customers, inventory.

**Sales Pipeline:** case_id = opportunity ID. Created → Discovery → Demo → Validation → Procure → Won/Lost. **Often snapshots.** Enrich: accounts, users.

**Incident Management:** case_id = ticket ID. Created → Assigned → Investigation → Resolved → Closed. Enrich: CMDB, employees.
