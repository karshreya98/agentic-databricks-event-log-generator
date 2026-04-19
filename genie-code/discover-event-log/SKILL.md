---
name: discover-event-log
description: Agentic event log discovery for process mining. Classifies tables as event sources or reference data, maps columns to process events, builds enriched event logs, and validates quality. Use when someone says "discover event log", "build event log", "process mining", "find events in", or wants to create a process event log from existing tables.
---

# Agentic Event Log Discovery

Build a process mining event log from tables in Unity Catalog.

**Announce at start:** "I'm running the discover-event-log skill to build a process event log from your catalog data."

## What This Skill Adds (on top of built-in skills)

You already have built-in skills for data discovery, table profiling, and SQL execution. This skill adds **process mining domain knowledge**:

- How to classify tables for process mining (event source vs reference vs aggregate)
- How to map columns to event log fields (case_id, activity, timestamp)
- How to handle snapshot tables that aren't event histories
- How to validate an event log for process mining readiness
- How to find and apply enrichments that add operational context
- Common process patterns (P2P, O2C, sales pipeline, ITSM)

**Use built-in skills for:** listing tables, profiling schemas, sampling data, understanding column statistics, exploring metadata. Don't reimplement what's already available.

## Input

Ask the user for (or infer from context):
1. **Catalogs/schemas** — where the source data lives
2. **Process hint** — what kind of process (e.g., "procure to pay", "sales pipeline")
3. **Output table** — where to write the event log (default: `process_mining.silver.event_log`)

---

## Phase 1: Discover & Classify

Use your built-in data discovery skills to profile the tables in the target schema. Then apply this process mining classification:

### Classification Rules

| Classification | What to look for | Action |
|---|---|---|
| **Event source** | Has date/timestamp columns + a high-cardinality ID column (unique count near row count) | Extract events from this table |
| **Reference data** | Has ID columns matching event tables, but no timestamps. Descriptive columns (name, industry, rating). | Use for enrichment joins |
| **Aggregate** | Very low row count. Dimension columns (Region, Month) but no granular unique ID. | **Skip** — not useful for event logs |
| **Duplicate** | Same columns as another table, similar row count. Name prefix `raw_` or `cleaned_`. | **Skip** — use the cleanest version |

### How to Identify Key Columns

- **case_id**: The column that groups all events into one process instance. High cardinality, appears across multiple tables. Examples: `po_number`, `order_id`, `opportunityid`, `incident_id`.
- **Timestamp columns**: Each one is a potential event. `created_at` → "Created", `approved_at` → "Approved", `closedate` → "Closed".
- **Status/stage columns**: Low cardinality with a progression (e.g., Discovery → Demo → Validation → Won/Lost). Indicates the table is a **snapshot** — see below.
- **Resource columns**: Moderate cardinality — who performed the action (user IDs, employee names).
- **Cost columns**: Numeric, positive range — amount associated with the event.

### Snapshot Tables (critical pattern)

Many operational tables store **one row per entity** with a `status` or `stage` column showing the CURRENT state — not the full event history.

Example: an `opportunity` table with `createddate`, `closedate`, and `stagename = "5. Closed Won"`.

How to handle:
- Create an event for each timestamp column (`createddate` → "Opportunity Created", `closedate` → final stage)
- For intermediate stages: if the entity passed through stages on the way to the current one, estimate timestamps proportionally between start and end dates
- **Always note this as an approximation** in the output

---

## Phase 2: Map & Test

For each event source table, define the extraction:
- **activity**: Human-readable name (e.g., "Create Purchase Order" not "PO_CREATED")
- **timestamp**: The column containing when the event happened
- **condition**: If the timestamp can be NULL, add a filter (e.g., `approved_at IS NOT NULL`)

**Test every mapping** by running the extraction SQL and checking:
- Timestamps look reasonable (not all NULL, not all the same)
- Case ID is populated
- Row count makes sense for the process

For enrichment tables:
- Test the join by checking match rate — what percentage of event log case IDs have a match?
- If < 10%, the join key is wrong. Common issue: event log has `accountid`, reference table has `id` — need `ON event.accountid = ref.id`
- Check for column name conflicts — alias if both tables have `name` (e.g., `account_name`, `rep_name`)

---

## Phase 3: Build & Validate

Combine all extractions with UNION ALL, add window functions for event ordering:

```sql
CREATE OR REPLACE TABLE <output_table> AS
SELECT
  case_id, activity, event_timestamp, resource, cost, source_table,
  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) AS event_rank,
  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(
    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)
  ) AS time_since_prev_seconds,
  COUNT(*) OVER (PARTITION BY case_id) AS case_event_count
FROM (<union_of_all_extractions_with_enrichment_joins>)
```

### Quality Gates (must ALL pass)

| Check | Pass condition |
|---|---|
| Total events | > 0 |
| Total cases | > 0 |
| Null case_ids | = 0 |
| Null timestamps | = 0 |
| Distinct activities | >= 2 |
| Avg events per case | > 1 |

**If any check fails → go back to Phase 2 and fix the mapping.**

---

## Phase 4: Report

```
Event Log Discovery Complete
================================
Process:      <name>
Output:       <table>
Events:       <count>
Cases:        <count>
Activities:   <list>
Enrichments:  <tables joined with match rates>

Quality:      All checks passed
Tables used:  <n> (list)
Tables skipped: <n> (with reasons)
```

---

## Error Recovery

| Error | Recovery |
|---|---|
| Join key mismatch (`accountid` vs `id`) | Check the reference table's primary key column, use explicit ON clause |
| Duplicate column after join | Alias the enrichment column (e.g., `a.name AS account_name`) |
| Empty extraction | Timestamp column has NULLs — add IS NOT NULL condition |
| Aggregate table misidentified as event source | Row count is very low with no unique ID — reclassify and skip |
| Snapshot table with no event history | Derive events from timestamp columns, note the approximation |

---

## Process Mining Patterns Reference

**Procure-to-Pay (P2P)**
- case_id: PO number
- Flow: Requisition → Approve → Purchase Order → Approve → Goods Receipt → Invoice → Clear → Payment
- Sources: purchase_orders, goods_receipts, invoices, payments
- Enrich with: supplier_master (credit risk, delivery rate), contracts (terms, amendments)

**Order-to-Cash (O2C)**
- case_id: Order ID
- Flow: Order → Credit Check → Pick → Pack → Ship → Invoice → Payment
- Sources: orders, credit_checks, shipments, invoices, payments
- Enrich with: customers (segment, lifetime value), inventory (stock level)

**Sales Pipeline**
- case_id: Opportunity ID
- Flow: Created → Discovery → Demo → Validation → Procure → Won/Lost
- **Often snapshot tables** — derive stages from timestamp columns
- Enrich with: accounts (industry, size, revenue), users (rep role, segment)

**Incident Management (ITSM)**
- case_id: Ticket ID
- Flow: Created → Assigned → Investigation → Resolved → Closed
- Sources: incidents, escalations, reassignments
- Enrich with: CMDB (config item details), employees (team, location)
