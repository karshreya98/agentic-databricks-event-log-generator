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

**Mandatory three (minimum for any process mining tool):**
- **case_id** (case key): High `unique_count` near `total_rows`, appears across tables. Examples: `po_number`, `opportunityid`, `incident_id`.
- **activity**: What happened. Derived from timestamp column names or status/stage values.
- **event_timestamp**: When it happened. `data_type` is date/timestamp.

**Activity-level attributes (enrich each event row):**
- **Resource / Performer**: Who executed the step. Moderate cardinality (user IDs, employee names).
- **Org unit / Department / Cost center**: Which team or department.
- **System / Channel**: Which source system produced this event (e.g., SAP, Ariba, Coupa).
- **Status**: Document status at this activity (e.g., approved, pending, rejected).
- **Flags**: Rework indicator, automation flag, manual vs system-generated.

**Case-level attributes (enrich from reference tables — same value for all events in a case):**
- **Value / Amount / Currency**: Order value, invoice amount.
- **Document type / Priority / Category**: Business classification.
- **Region / Country**: Geographic dimension.
- **Customer / Vendor / Supplier**: Business partner details (name, industry, risk rating).
- **SLA dates / Creation date / Due date**: Time-based case attributes.

These align with what tools like Celonis call "advanced columns" — added on top of the mandatory three to enable richer analysis (filtering, grouping, root cause). **Always look for as many of these as the source data provides.**

### Snapshot Tables (critical pattern)

Tables with one row per entity + a `stage`/`status` column showing CURRENT state. Example: `opportunity` with `createddate`, `closedate`, `stagename`.

Handle by: creating an event per timestamp column, deriving intermediate stages proportionally. **Always note this as an approximation.**

---

## Phase 2: Map & Test

For each event source, define: activity name, timestamp column, optional condition/resource/cost.

**Test every mapping with `execute_sql`** — run the extraction and verify timestamps look reasonable, case_id is populated, row count makes sense.

**Enrichments (case-level attributes):**

Search for reference/master data tables that add context to each case. These become case-level attributes — the same value for every event in a case:

- Supplier master → `credit_risk_rating`, `on_time_delivery_rate`, `country`
- Customer master → `industry`, `segment`, `lifetime_value`
- Contract data → `contract_type`, `amendment_count`, `payment_terms_days`
- User/employee → `rep_name`, `title`, `role`, `region`

For each candidate: test join match rate with `execute_sql`. Handle column name mismatches (`accountid` vs `id`) and conflicts (alias duplicates like `account_name`, `rep_name`). If the event log doesn't have the join key directly, look for bridge tables (e.g., event_log → purchase_orders → supplier_master).

---

## Phase 3: Build & Validate

Build with `execute_sql`. Include as many columns as the data provides:

```sql
CREATE OR REPLACE TABLE <output_table> AS
SELECT
  -- Mandatory three
  case_id,
  activity,
  event_timestamp,

  -- Activity-level attributes
  resource,                              -- who performed it
  department,                            -- org unit
  source_table AS source_system,         -- which system
  cost,                                  -- event cost if available

  -- Computed metrics
  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) AS event_rank,
  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(
    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
  ) AS time_since_prev_seconds,
  COUNT(*) OVER (PARTITION BY case_id) AS case_event_count
  -- Case-level attributes (from enrichment joins)
  -- e.g., supplier_name, credit_risk_rating, contract_type, account_industry, rep_name
FROM (<union_with_enrichment_joins>)
```

Use `manage_uc_objects` to create catalog/schema if needed.

**Column naming convention:** Prefix enrichment columns with their source to avoid ambiguity: `supplier_name`, `account_industry`, `rep_role`, `contract_type`.

### Quality Gates

| Check | Pass |
|---|---|
| Total events | > 0 |
| Null case_ids / timestamps | = 0 |
| Distinct activities | >= 2 |
| Avg events per case | > 1 |

**If any fails → back to Phase 2.**

---

## Phase 3b: OCEL Output (Object-Centric, optional)

If the user requests OCEL output, or if the data naturally has multiple object types per event, generate an **OCEL 2.0** output alongside the traditional event log.

### When to suggest OCEL

- Multiple ID columns exist in the event data (e.g., `po_number`, `invoice_id`, `supplier_id`)
- Events naturally relate to more than one object (e.g., a goods receipt relates to both a PO and a supplier)
- The user mentions "object-centric", "OCEL", or "multi-object"

### OCEL 2.0 Schema (3 tables)

**Events table** (`<output>_ocel_events`):
```sql
CREATE OR REPLACE TABLE <output>_ocel_events AS
SELECT
  event_id,           -- unique per event (e.g., ROW_NUMBER or concat of source+id)
  activity,
  event_timestamp,
  resource,
  cost,
  source_table
FROM (<union_of_all_extractions>)
```

**Objects table** (`<output>_ocel_objects`):
```sql
CREATE OR REPLACE TABLE <output>_ocel_objects AS
-- Collect all unique object IDs across all types
SELECT object_id, object_type FROM (
  SELECT DISTINCT po_number AS object_id, 'PurchaseOrder' AS object_type FROM ...
  UNION ALL
  SELECT DISTINCT invoice_id, 'Invoice' FROM ...
  UNION ALL
  SELECT DISTINCT supplier_id, 'Supplier' FROM ...
  -- etc. for each ID column discovered
)
```

**Event-to-Object relationships** (`<output>_ocel_e2o`):
```sql
CREATE OR REPLACE TABLE <output>_ocel_e2o AS
-- Each event links to ALL objects it relates to (many-to-many)
SELECT event_id, po_number AS object_id, 'PurchaseOrder' AS object_type FROM events WHERE po_number IS NOT NULL
UNION ALL
SELECT event_id, invoice_id, 'Invoice' FROM events WHERE invoice_id IS NOT NULL
UNION ALL
SELECT event_id, supplier_id, 'Supplier' FROM events WHERE supplier_id IS NOT NULL
-- etc.
```

### Key difference from traditional

Traditional: each event has ONE case_id → forces a single perspective
OCEL: each event links to MULTIPLE objects → preserves all relationships

Example: a "Post Goods Receipt" event in traditional PM belongs to `po_number = P2P-001`.
In OCEL, the same event links to: PO `P2P-001` + Supplier `SUP-042` + Warehouse `WH-East`.

### OCEL quality checks

- Events table has unique event_ids
- E2O table has no orphan event_ids or object_ids
- Every event links to at least one object
- Object types are consistent and meaningful

---

## Phase 4: Report

```
Event Log Discovery Complete
================================
Process:      <name>
Output:       <table>
Events:       <count>
Cases:        <count>
Activities:   <count> (<list>)

Columns:
  Mandatory:        case_id, activity, event_timestamp
  Activity-level:   resource, department, source_system, cost
  Computed:         event_rank, time_since_prev_seconds, case_event_count
  Case-level:       <enrichment columns + match rates>

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
