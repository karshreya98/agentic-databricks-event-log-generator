---
name: discover-event-log
description: Agentic event log discovery for process mining. Scans Unity Catalog tables, profiles with rich metadata, iteratively reasons about mappings, validates extractions, and produces a tested YAML config. Use when someone says "discover event log", "build event log from", "process mining on", "find events in", or wants to create a process event log from existing tables.
user-invocable: true
---

# Agentic Event Log Discovery

Build a process mining event log from any tables in Unity Catalog — with iterative reasoning, rich metadata profiling, and validation.

**Announce at start:** "I'm using the discover-event-log skill to build a process event log from your catalog data."

## How This Works

This skill uses Databricks MCP tools iteratively — not a single LLM call:
- **Profiles tables with `get_table_details`** — gets schema, row counts, sample values, unique counts, and value distributions in one call
- **Reads table/column tags with `manage_uc_tags`** — surfaces PII markers, business classifications
- **Tests extractions with `execute_sql`** — validates every mapping with real queries
- **Validates joins** and catches column mismatches before committing
- **Self-corrects** when a mapping doesn't work
- **Checks quality gates** and iterates until the event log passes

## Prerequisites Check (run this FIRST)

Before doing anything else, verify the Databricks MCP tools are available by calling:

```
get_best_warehouse()
```

**If this works** → the Databricks AI Dev Kit is installed. Continue to Phase 1.

**If this fails with "unknown tool" or "InputValidationError"** → the Databricks MCP tools are not installed. Stop and tell the user:

> The `/discover-event-log` skill requires the Databricks AI Dev Kit, which provides the MCP tools for querying Unity Catalog.
>
> To install it, run:
> ```
> bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
> ```
>
> Then restart Claude Code and try again. See the [AI Dev Kit README](https://github.com/databricks-solutions/ai-dev-kit) for details.

**If this fails with an auth error** → the tools exist but Databricks isn't authenticated. Tell the user:

> Databricks authentication is not configured. Run:
> ```
> databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
> ```
> Make sure the `[DEFAULT]` profile in `~/.databrickscfg` points to your workspace.

## Prerequisites (after check passes)

- Authenticated Databricks workspace
- Access to a SQL warehouse (auto-detected via `get_best_warehouse`)
- Tables with operational/transactional data in Unity Catalog

## Input

Ask the user for (or infer from context):
1. **Catalogs/schemas to scan** — where the source data lives
2. **Process hint** — what kind of process (e.g., "procure to pay", "sales pipeline", "incident management")
3. **Output table** — where to write the event log (default: `process_mining.silver.event_log`)

---

## Phase 1: Catalog Scan & Table Profiling

### Step 1.1: List schemas and tables

Use **`manage_uc_objects`** (not raw SQL) to list what's available:

```
manage_uc_objects(object_type="schema", action="list", catalog_name="<catalog>")
```

Then for tables in each schema, use **`get_table_details`** — this is the key tool. One call returns:

### Step 1.2: Profile all tables in one call

```
get_table_details(catalog="<catalog>", schema="<schema>", table_stat_level="SIMPLE")
```

This returns **per table**:
- `total_rows` — row count
- `comment` — table description (if set, often explains the table's purpose)
- `ddl` — full CREATE TABLE statement
- Per column: `name`, `data_type`, `samples` (actual values), `unique_count`, `value_counts` (for low-cardinality columns), `min`/`max`/`avg` (for numerics)

This replaces `DESCRIBE TABLE` + `SELECT COUNT(*)` + `SELECT * LIMIT 5` — **three queries in one tool call**.

### Step 1.3: Check for tags

Use **`manage_uc_tags`** to surface any existing metadata:

```
manage_uc_tags(action="query_column_tags", catalog_filter="<catalog>")
```

Tags like `pii=true`, `classification=confidential`, `business_key=true` provide extra signals about column purpose.

### Step 1.4: Classify tables

Using the rich metadata from `get_table_details`, classify each table:

| Classification | How to detect from metadata | Action |
|---|---|---|
| **Event source** | Has `date`/`timestamp` columns + a high-cardinality string ID column (`unique_count` close to `total_rows`) | Use for event extraction |
| **Reference/master data** | Has ID columns matching event tables, but no timestamps. `comment` describes it as master/reference data. Low `total_rows` relative to event tables. | Use for enrichment |
| **Aggregate/summary** | Very low `total_rows`. Columns are dimensions (Region, Month) with low `unique_count`. No high-cardinality ID column. | **Skip** |
| **Duplicate (raw/cleaned)** | Same columns as another table, similar `total_rows`. Name starts with `raw_` or `cleaned_`. | **Skip** — use the cleanest version |

**Key reasoning from metadata:**
- Column with `unique_count` ≈ `total_rows` → likely a primary key / case ID
- Column with `value_counts` showing a small set of categorical values (e.g., stages) → activity/status column
- Column with `data_type: "date"` or `"timestamp"` → event timestamp candidate
- `total_rows` in the single digits or dozens → aggregate table, skip it
- `comment` field often directly tells you what the table is for — read it

---

## Phase 2: Mapping Discovery

### Step 2.1: Identify the case ID

From the `get_table_details` output:
- Find columns where `unique_count` is close to `total_rows` — these are candidate primary keys
- Look for columns with the same name across multiple tables (e.g., `po_number`, `order_id`) — these link tables together
- The case ID is the column that groups all events for one process instance

### Step 2.2: Map events

For each event source table:
- **Timestamp columns** → each becomes an event
- **Activity name** → derive from column name (e.g., `approved_at` → "Approved", `closedate` → "Closed")
- **Condition** → if a timestamp column's `unique_count` < `total_rows`, some rows have NULLs. Add `<col> IS NOT NULL`.
- **Resource** → look for columns with moderate cardinality (user IDs, names)
- **Cost** → numeric columns with `avg` in a reasonable range

For **snapshot tables** (one row per entity with a `stage`/`status` column):
- Check the stage column's `value_counts` — does it show a progression? (e.g., Discovery → Demo → Validation → Procure → Closed)
- You can derive intermediate stage events by estimating timestamps proportionally between `createddate` and `closedate`. Note this as an approximation.

### Step 2.3: Test each extraction with `execute_sql`

**CRITICAL: Do not guess. Test every mapping with a real query.**

```sql
SELECT
  <case_id> as case_id,
  '<activity_name>' as activity,
  <timestamp_col> as event_timestamp
FROM <table>
WHERE <timestamp_col> IS NOT NULL
LIMIT 10
```

Check: timestamps look reasonable, case_id is populated, row count makes sense.

### Step 2.4: Identify enrichment tables

From the profiling data:
- Reference tables have ID columns matching event table foreign keys
- **Check join key compatibility**: if event table has `accountid` and reference table has `id`, you need to join `ON event.accountid = ref.id` and note the mismatch
- **Check for column name conflicts**: if both tables have a `name` column, alias the enrichment column (e.g., `account_name`)
- **Test the join with `execute_sql`** to verify match rate

---

## Phase 3: Build & Validate

### Step 3.1: Generate the YAML config

Based on validated mappings, produce a config. See `references/yaml-schema.md` for the full schema.

### Step 3.2: Build a test event log with `execute_sql`

Run a UNION ALL query combining all event extractions:

```sql
SELECT case_id, activity, event_timestamp, COUNT(*) as cnt
FROM (
  -- all source extractions UNION'd together
)
GROUP BY case_id, activity, event_timestamp
ORDER BY case_id, event_timestamp
LIMIT 20
```

### Step 3.3: Validate quality with `execute_sql`

```sql
-- Total events and cases
SELECT COUNT(*) as events, COUNT(DISTINCT case_id) as cases FROM (<union_query>)

-- Events per case distribution
SELECT MIN(cnt) as min_events, ROUND(AVG(cnt),1) as avg_events, MAX(cnt) as max_events
FROM (SELECT case_id, COUNT(*) as cnt FROM (<union_query>) GROUP BY case_id)

-- Activity distribution
SELECT activity, COUNT(*) as cnt FROM (<union_query>) GROUP BY activity ORDER BY cnt DESC

-- Null check
SELECT
  SUM(CASE WHEN case_id IS NULL THEN 1 ELSE 0 END) as null_case_ids,
  SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps
FROM (<union_query>)
```

**Quality gates:**
- Events > 0 and Cases > 0
- No null case_ids or timestamps
- At least 2 distinct activities
- Average events per case > 1

If any check fails → go back to Phase 2 and fix the mapping.

---

## Phase 4: Save & Report

### Step 4.1: Create the output table with `execute_sql`

```sql
CREATE OR REPLACE TABLE <output_table> AS
SELECT
  case_id, activity, event_timestamp, resource, cost, source_table,
  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) as event_rank,
  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(
    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
  ) as time_since_prev_seconds,
  COUNT(*) OVER (PARTITION BY case_id) as case_event_count
  -- enrichment columns with aliases to avoid conflicts
FROM (<union_query_with_enrichment_joins>)
```

Use **`manage_uc_objects`** to create catalog/schema if needed (not raw SQL DDL).

### Step 4.2: Save the YAML config

Write the validated config to a local file with the Write tool.

### Step 4.3: Report results

```
Event Log Discovery Complete
================================
Process:      <name>
Output:       <table>
Events:       <count>
Cases:        <count>
Activities:   <list>
Enrichments:  <list of tables joined with match rates>

Quality:
  - Null case IDs:    0
  - Null timestamps:  0
  - Avg events/case:  <n>
  - Activity count:   <n>

Tables scanned:    <n>
Tables used:       <n> (list)
Tables skipped:    <n> (list with reasons)

YAML config saved to: <path>
```

---

## Tool Reference

| Task | Tool | Why better than SQL |
|---|---|---|
| List catalogs/schemas | `manage_uc_objects` | Structured output, no parsing |
| Profile all tables in a schema | `get_table_details` | One call returns schema + row counts + samples + unique counts + value distributions + comments + DDL |
| Read table/column tags | `manage_uc_tags` | Surfaces PII, business keys, classifications |
| Create catalog/schema | `manage_uc_objects` | Proper SDK call, handles permissions |
| Test extractions | `execute_sql` | Run queries to validate mappings |
| Build output table | `execute_sql` | CTAS for the final event log |
| Find warehouse | `get_best_warehouse` | Auto-selects running warehouse |

---

## Error Recovery

| Error | Recovery |
|---|---|
| Table not found | Remove from config, continue |
| Column not found | Re-check with `get_table_details`, fix name |
| Join key mismatch (`accountid` vs `id`) | Check reference table's high-cardinality column, rename in join |
| Duplicate column after join | Alias enrichment columns (e.g., `a.name as account_name`) |
| Empty extraction | Check condition, look at `unique_count` vs `total_rows` for nulls |
| Aggregate table misidentified as event source | Check `total_rows` — if very low, and no high-cardinality ID, skip it |

---

## References

- `references/yaml-schema.md` — Full YAML config schema
- `references/process-templates.md` — Common process patterns (P2P, O2C, ITSM, Sales Pipeline)
- The `eventlog/` Python package in this repo can consume the generated YAML config
