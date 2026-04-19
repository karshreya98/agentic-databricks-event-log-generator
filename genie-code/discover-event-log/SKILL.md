---
name: discover-event-log
description: Agentic event log discovery for process mining. Scans Unity Catalog tables, profiles metadata, reasons about mappings, tests extractions with SQL, and produces a validated event log. Use when someone says "discover event log", "build event log", "process mining", "find events in", or wants to create a process event log from existing tables.
---

# Agentic Event Log Discovery

Build a process mining event log from any tables in Unity Catalog — with iterative reasoning, metadata profiling, and validation.

**Announce at start:** "I'm running the discover-event-log skill to build a process event log from your catalog data."

## How This Works

This skill works iteratively — not a single pass:
- **Profiles tables** using UC metadata (schema, row counts, sample values, cardinality, column descriptions)
- **Reasons** about which tables are event sources vs reference data vs aggregates
- **Tests every mapping** with real SQL before committing
- **Validates joins** and catches column mismatches
- **Self-corrects** when a mapping doesn't work
- **Checks quality gates** and iterates until the event log passes

## Input

Ask the user for (or infer from context):
1. **Catalogs/schemas to scan** — where the source data lives
2. **Process hint** — what kind of process (e.g., "procure to pay", "sales pipeline", "incident management")
3. **Output table** — where to write the event log (default: `process_mining.silver.event_log`)

---

## Phase 1: Catalog Scan & Table Profiling

### Step 1.1: List tables in the target schema

```sql
SHOW TABLES IN <catalog>.<schema>
```

### Step 1.2: Profile each table

For every candidate table, gather metadata:

```sql
-- Schema and column types
DESCRIBE TABLE <catalog>.<schema>.<table>

-- Row count and cardinality of key columns
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT <candidate_id_col>) AS id_cardinality
FROM <catalog>.<schema>.<table>

-- Sample 5 rows to understand actual content
SELECT * FROM <catalog>.<schema>.<table> LIMIT 5

-- For low-cardinality columns (status/stage), get value distribution
SELECT <status_col>, COUNT(*) AS cnt
FROM <catalog>.<schema>.<table>
GROUP BY <status_col>
ORDER BY cnt DESC
```

### Step 1.3: Classify tables

| Classification | How to detect | Action |
|---|---|---|
| **Event source** | Has date/timestamp columns + a high-cardinality ID column (unique count close to row count) | Use for event extraction |
| **Reference/master data** | Has ID columns matching event tables, but no timestamps. Descriptive/dimensional columns. | Use for enrichment |
| **Aggregate/summary** | Very low row count. Dimension columns (Region, Month) with low cardinality. No unique ID. | **Skip** |
| **Duplicate (raw/cleaned)** | Same columns as another table, similar row count. Name starts with `raw_` or `cleaned_`. | **Skip** — use the cleanest version |

**Key reasoning:**
- Column with unique count close to row count → likely a primary key / case ID
- Column with a small set of categorical values (e.g., stages like Discovery→Demo→Validation→Procure→Won/Lost) → activity/status column
- Date/timestamp columns → event timestamp candidates
- Very few rows (single digits or dozens) → aggregate table, skip

---

## Phase 2: Mapping Discovery

### Step 2.1: Identify the case ID

- Find columns appearing across multiple tables (e.g., `order_id`, `po_number`, `opportunityid`)
- Verify cardinality: `SELECT COUNT(DISTINCT <candidate>) FROM <table>` — should match expected case count

### Step 2.2: Map events

For each event source table:
- **Timestamp columns** → each becomes a separate event
- **Activity name** → derive from column name (e.g., `approved_at` → "Approved", `closedate` → "Closed")
- **Condition** → if timestamp column has NULLs, add `WHERE <col> IS NOT NULL`
- **Resource** → column with moderate cardinality (user IDs, names)
- **Cost** → numeric columns in a reasonable range

For **snapshot tables** (one row per entity with a stage/status column):
- Check if stages show a progression
- Derive intermediate stage events by estimating timestamps proportionally between start and end dates
- Note this as an approximation in the output

### Step 2.3: Test each extraction

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

For each reference table:
- Find join key (column matching an event log foreign key)
- **Test the join match rate:**

```sql
SELECT
  COUNT(DISTINCT e.<case_id>) AS total_cases,
  COUNT(DISTINCT CASE WHEN r.<key> IS NOT NULL THEN e.<case_id> END) AS matched
FROM <event_table> e
LEFT JOIN <ref_table> r ON e.<fk> = r.<pk>
```

- If match rate < 10%, the join key is wrong — try alternatives (`id`, `<table>_id`)
- **Check for column name conflicts** — alias enrichment columns if they'd collide (e.g., both tables have `name`)

---

## Phase 3: Build & Validate

### Step 3.1: Build the full event log query

Combine all extractions with UNION ALL:

```sql
SELECT case_id, activity, event_timestamp FROM source1 WHERE ...
UNION ALL
SELECT case_id, activity, event_timestamp FROM source2 WHERE ...
```

### Step 3.2: Validate quality

```sql
-- Must pass ALL gates
SELECT
  COUNT(*) AS events,                          -- > 0
  COUNT(DISTINCT case_id) AS cases,            -- > 0
  COUNT(DISTINCT activity) AS activities,      -- >= 2
  SUM(CASE WHEN case_id IS NULL THEN 1 ELSE 0 END) AS null_ids,        -- = 0
  SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_ts  -- = 0
FROM (<union_query>)
```

Also check events per case:
```sql
SELECT ROUND(AVG(cnt), 1) AS avg_events_per_case  -- should be > 1
FROM (SELECT case_id, COUNT(*) AS cnt FROM (<union_query>) GROUP BY case_id)
```

**If any check fails → go back to Phase 2 and fix the mapping.**

---

## Phase 4: Save & Report

### Step 4.1: Create output table

```sql
CREATE OR REPLACE TABLE <output_table> AS
SELECT
  case_id, activity, event_timestamp, resource, cost, source_table,
  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) AS event_rank,
  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(
    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
  ) AS time_since_prev_seconds,
  COUNT(*) OVER (PARTITION BY case_id) AS case_event_count
  -- plus enrichment columns with aliases
FROM (<union_with_enrichment_joins>)
```

### Step 4.2: Report results

```
Event Log Discovery Complete
================================
Process:      <name>
Output:       <table>
Events:       <count>
Cases:        <count>
Activities:   <list>
Enrichments:  <tables joined with match rates>

Quality:
  Null case IDs:    0
  Null timestamps:  0
  Avg events/case:  <n>
  Activity count:   <n>

Tables scanned:    <n>
Tables used:       <n>
Tables skipped:    <n> (with reasons)
```

---

## Error Recovery

| Error | Recovery |
|---|---|
| Column not found | Re-check schema with DESCRIBE, fix column name |
| Join key mismatch (`accountid` vs `id`) | Check reference table's primary key, use alias in JOIN ON clause |
| Duplicate column after join | Alias enrichment columns (e.g., `a.name AS account_name`) |
| Empty extraction | Check for NULLs in timestamp column, adjust condition |
| Aggregate table misidentified | Check row count — if very low with no unique ID, skip it |

---

## Reference: Common Process Patterns

**Procure-to-Pay:** case_id = PO number. Flow: PR → Approve → PO → Approve → Goods Receipt → Invoice → Clear → Payment. Sources: purchase_orders, goods_receipts, invoices, payments. Enrich with: supplier_master, contracts.

**Order-to-Cash:** case_id = order ID. Flow: Order → Credit Check → Pick → Pack → Ship → Invoice → Payment. Sources: orders, shipments, invoices, payments. Enrich with: customers, inventory.

**Sales Pipeline:** case_id = opportunity ID. Flow: Created → Discovery → Demo → Validation → Procure → Won/Lost. Often snapshot tables — derive stages from timestamps. Enrich with: accounts, users.

**Incident Management:** case_id = ticket ID. Flow: Created → Assigned → Investigation → Resolved → Closed. Sources: incidents, escalations, reassignments. Enrich with: CMDB, employees.
