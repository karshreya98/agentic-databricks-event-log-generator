# Example: Running Discovery with Claude Code

## Prerequisites

- Ran `01_create_source_tables.py` on a Databricks cluster
- Claude Code + AI Dev Kit configured (run `./scripts/setup-claude-code.sh`)

## Run

```bash
cd agentic-databricks-event-log-generator
claude
```

```
/discover-event-log
```

> Build both a traditional and OCEL event log from process_mining.erp_raw — it's a procure-to-pay process. Use process_mining.reference for enrichment. Save to process_mining.silver.

## What to expect

The skill will:

1. **Profile 5 operational + 3 reference tables** via `get_table_details`

2. **Classify:**
   - `purchase_requisitions` → EVENT_SOURCE (pr_id + created_at/approved_at)
   - `purchase_orders` → EVENT_SOURCE (po_id + created_at/approved_at, links to pr_id + supplier_id + contract_id)
   - `goods_receipts` → EVENT_SOURCE (gr_id + posting_date, links to po_id)
   - `invoices` → EVENT_SOURCE (invoice_id + received_date/cleared_date, links to po_id + supplier_id)
   - `payments` → EVENT_SOURCE (payment_id + payment_date, links to invoice_id + po_id + supplier_id)
   - `supplier_master` → ENRICHMENT
   - `contracts` → ENRICHMENT
   - `cost_centers` → ENRICHMENT

3. **Discover the many-to-many relationships:**
   - PR → 1-3 POs (split purchases)
   - PO → 1-3 GRs (partial deliveries)
   - PO → 1-2 Invoices (split billing)
   - This is what makes OCEL output valuable

4. **Build traditional event log:**
   - Pick `po_id` as case_id (most central object)
   - UNION ALL extractions + enrichment joins
   - ~12 activities, ~15,000+ events

5. **Build OCEL tables:**
   - Events table: one row per event with all attributes
   - Objects table: unique PRs + POs + GRs + Invoices + Suppliers + Contracts
   - E2O table: each event links to all objects it touches

6. **Validate both** and report

## Comparing the outputs

```sql
-- Traditional: how many events per PO?
SELECT case_id, COUNT(*) AS events
FROM process_mining.silver.traditional_event_log
GROUP BY case_id ORDER BY events DESC LIMIT 5

-- OCEL: how many objects does each event link to?
SELECT links, COUNT(*) AS events FROM (
  SELECT event_id, COUNT(*) AS links FROM process_mining.silver.p2p_ocel_e2o GROUP BY event_id
) GROUP BY links ORDER BY links

-- OCEL: see the full P2P process from a supplier's perspective
SELECT e.activity, e.event_timestamp, e2o.object_id
FROM process_mining.silver.p2p_ocel_events e
JOIN process_mining.silver.p2p_ocel_e2o e2o ON e.event_id = e2o.event_id
WHERE e2o.object_type = 'Supplier' AND e2o.object_id = 'SUP-0042'
ORDER BY e.event_timestamp
```
