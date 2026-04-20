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

## Phase 2b: Review & Confirm (MANDATORY)

**Before building, present the proposed mappings to the user and ask for confirmation.**

Show a summary table:

```
Proposed Event Log Mappings
============================
Case ID:    po_number (from purchase_orders)
Output:     process_mining.silver.event_log

Events:
  Source Table               Activity                  Timestamp       Condition          Rows
  ─────────────              ────────                  ─────────       ─────────          ────
  purchase_orders            Create Purchase Order     created_at                         5,000
  purchase_orders            Approve Purchase Order    approved_at     IS NOT NULL        4,750
  goods_receipts             Post Goods Receipt        posting_date                       3,500
  invoices                   Receive Invoice           received_date                      3,200
  invoices                   Clear Invoice             cleared_date    IS NOT NULL        2,720
  payments                   Process Payment           payment_date                       2,700

Enrichments:
  Table                  Join Key          Match Rate   Columns Added
  ─────                  ────────          ──────────   ─────────────
  supplier_master        supplier_id       92%          credit_risk_rating, on_time_delivery_rate, country
  contracts              contract_id       88%          contract_type, amendment_count, payment_terms_days

Format: Traditional (single case_id)
```

Then ask: **"Does this look right? Any mappings to change, add, or remove before I build?"**

**If you detected multiple object types linking to each event** (e.g. events relate to both `PurchaseOrder` and `Invoice` and `Supplier`), also ask:

> "I noticed events link to multiple object types: `PurchaseOrder`, `Invoice`, `Supplier`. Do you want me to also build **OCEL 2.0 output** (3 extra tables that preserve all object relationships, for object-centric process mining)? (yes / no — default: no)"

Do **not** build OCEL unless the user explicitly opts in. Most users only need the traditional event log; OCEL adds 3 extra tables that most PM tools don't consume.

**Wait for the user to confirm.** If they say:
- "yes" / "looks good" → proceed to Phase 3 (traditional only, unless OCEL was explicitly requested)
- "change X" → adjust the mapping and re-show
- "remove the enrichment from contracts" → drop it
- "add department from the cost_centers table" → add it
- "that's not the right case ID" → go back to Phase 2
- "yes, and build OCEL too" → proceed to Phase 3 + Phase 3b

This is the human-in-the-loop checkpoint. The agent proposes, the user validates.

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

### Post-Build Validation (user can run independently)

After saving, provide the user with standalone validation queries they can run in a notebook or SQL editor to verify the output without trusting the agent's report:

```sql
-- 1. Basic stats
SELECT COUNT(*) AS events, COUNT(DISTINCT case_id) AS cases,
       COUNT(DISTINCT activity) AS activities
FROM <output_table>;

-- 2. Activity distribution — do the counts make sense?
SELECT activity, COUNT(*) AS cnt FROM <output_table> GROUP BY activity ORDER BY cnt DESC;

-- 3. Sample a single case — does the process flow look right?
SELECT case_id, activity, event_timestamp, resource
FROM <output_table>
WHERE case_id = (SELECT case_id FROM <output_table> LIMIT 1)
ORDER BY event_timestamp;

-- 4. Null check
SELECT
  SUM(CASE WHEN case_id IS NULL THEN 1 ELSE 0 END) AS null_case_ids,
  SUM(CASE WHEN activity IS NULL THEN 1 ELSE 0 END) AS null_activities,
  SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps
FROM <output_table>;

-- 5. Enrichment coverage — what % of rows have enrichment data?
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN <enrichment_col> IS NOT NULL THEN 1 ELSE 0 END) AS enriched,
  ROUND(SUM(CASE WHEN <enrichment_col> IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct
FROM <output_table>;

-- 6. Duplicate check
SELECT case_id, activity, event_timestamp, COUNT(*) AS dupes
FROM <output_table>
GROUP BY case_id, activity, event_timestamp
HAVING COUNT(*) > 1;
```

**Always provide these queries in the report output** so the user can validate independently.

---

## Phase 3b: OCEL Output (Object-Centric, opt-in only)

**Only run this phase if the user explicitly opted in** at the Phase 2 checkpoint (or at the start of the request: "include OCEL", "object-centric", "OCEL 2.0"). If they said no or didn't answer the OCEL question, skip this phase.

### When to offer OCEL at the Phase 2 checkpoint

Offer OCEL as an opt-in question only if all of the following are true:
- Multiple ID columns exist in the event data (e.g., `po_number`, `invoice_id`, `supplier_id`)
- Events naturally relate to more than one object (e.g., a goods receipt relates to both a PO and a supplier)

If there's only one object type per event, don't even mention OCEL — it adds no value.

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

Validate independently:
  SELECT activity, COUNT(*) FROM <output> GROUP BY activity ORDER BY COUNT(*) DESC;
  SELECT * FROM <output> WHERE case_id = '<sample>' ORDER BY event_timestamp;
```

**Always ask:** "Want me to adjust anything, or does this look correct?"

---

## Error Recovery

### Agent-side (automatic)

| Error | Recovery |
|---|---|
| Join key mismatch | Check reference table's primary key, use explicit ON clause |
| Duplicate column after join | Alias enrichment columns |
| Empty extraction | Add IS NOT NULL condition on timestamp |
| Aggregate misidentified | Low row count + no unique ID → reclassify and skip |
| Snapshot table | Derive events from timestamps, note approximation |
| Quality gate fails | Go back to Phase 2, fix the specific mapping, rebuild |

### User-side (corrections)

If the user identifies an issue after the build (via the validation queries or by inspecting the data), they can tell the agent what to fix:

- *"The case ID should be pr_id, not po_id"* → agent re-runs with the corrected case ID
- *"Remove the Approve PO event — our process doesn't have that step"* → agent drops it and rebuilds
- *"The timestamps on goods_receipts look wrong — they're before the PO creation"* → agent investigates the out-of-order issue, adds a filter or flags affected rows
- *"Enrichment coverage is only 50% — can you find why?"* → agent checks for null join keys, supplier_id typos, case sensitivity mismatches
- *"Add the warehouse column from goods_receipts as an activity attribute"* → agent adds it to the extraction

**The agent should always be ready to iterate.** Event log creation is rarely right on the first pass. The review step (Phase 2b) catches most issues upfront, but post-build corrections are normal.

### Fallback: manual SQL

If the agent can't resolve an issue, provide the user with the generated SQL so they can modify it directly:

- Show the full UNION ALL query that built the event log
- Show each enrichment JOIN with match rates
- The user can copy, edit, and run it in a notebook

**Never leave the user stuck.** If the agent can't fix it, give them the SQL to fix it themselves.

---

## Process Mining Patterns Reference

**Procure-to-Pay:** case_id = PO number. PR → Approve → PO → GR → Invoice → Clear → Payment. Enrich: supplier_master, contracts.

**Order-to-Cash:** case_id = order ID. Order → Credit → Pick/Pack/Ship → Invoice → Payment. Enrich: customers, inventory.

**Sales Pipeline:** case_id = opportunity ID. Created → Discovery → Demo → Validation → Procure → Won/Lost. **Often snapshots.** Enrich: accounts, users.

**Incident Management:** case_id = ticket ID. Created → Assigned → Investigation → Resolved → Closed. Enrich: CMDB, employees.
