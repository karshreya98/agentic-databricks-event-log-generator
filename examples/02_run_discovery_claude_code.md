# Example: Running Discovery with Claude Code

## Prerequisites

- Ran `01_create_source_tables.py` on a Databricks cluster
- Claude Code installed + AI Dev Kit configured (run `./setup.sh` from repo root)

## Steps

### 1. Start Claude Code from the repo directory

```bash
cd agentic-databricks-event-log-generator
claude
```

### 2. Invoke the skill

```
/discover-event-log
```

### 3. Tell it what to build

> Build an event log from process_mining.erp_raw — it's a procure-to-pay process.
> Include enrichments from process_mining.reference.
> Save to process_mining.silver.p2p_event_log.

### 4. What happens

Claude follows the skill workflow:

**Phase 1 — Discover:**
Calls `get_table_details("process_mining", "erp_raw")` — gets schema, row counts, samples, cardinality for all 4 tables in one call. Also profiles `process_mining.reference`.

Classifies:
- `purchase_orders` → EVENT_SOURCE (has `created_at`, `approved_at`, `po_number` with high cardinality)
- `goods_receipts` → EVENT_SOURCE (has `posting_date`, `po_number`)
- `invoices` → EVENT_SOURCE (has `received_date`, `cleared_date`, `po_number`)
- `payments` → EVENT_SOURCE (has `payment_date`, `po_number`)
- `supplier_master` → ENRICHMENT (has `supplier_id`, no timestamps)
- `contracts` → ENRICHMENT (has `contract_id`, no timestamps)

**Phase 2 — Map & Test:**
- Case ID: `po_number` (appears in all 4 event tables)
- Maps: `created_at` → "Create Purchase Order", `approved_at` → "Approve Purchase Order" (with IS NOT NULL), etc.
- Tests each extraction with `execute_sql`
- Tests enrichment joins: `supplier_id` match rate, `contract_id` match rate
- Detects and handles any column name conflicts

**Phase 3 — Build:**
- UNION ALL of all extractions + enrichment joins
- Validates quality gates (no nulls, 6+ activities, ~6 events/case)
- Creates `process_mining.silver.p2p_event_log`

**Phase 4 — Report:**
```
Event Log Discovery Complete
================================
Process:      Procure-to-Pay
Output:       process_mining.silver.p2p_event_log
Events:       ~27,000
Cases:        ~5,000
Activities:   6 (Create PO → Approve PO → Goods Receipt → Invoice → Clear → Payment)
Enrichments:  supplier_master (credit risk, delivery rate, country)
              contracts (amendment count, payment terms)
```

### 5. Consume the result

**Delta Share to Celonis:**
```sql
CREATE SHARE process_mining_share;
ALTER SHARE process_mining_share ADD TABLE process_mining.silver.p2p_event_log;
```

**Deploy pm4py app:**
```bash
cd consumers/pm4py-app
# Update app.yaml with your warehouse ID and table name
databricks apps create process-mining --app-source .
```
