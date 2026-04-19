# Example: Running Discovery with Genie Code

## Prerequisites

- Ran `01_create_source_tables.py` on a Databricks cluster
- Ran `./install-genie-code.sh` from repo root

## Steps

### 1. Open Genie Code

Go to your Databricks workspace → Genie Code. Make sure you're in **Agent mode**.

### 2. Invoke the skill

> @discover-event-log Build an event log from process_mining.erp_raw — it's a procure-to-pay process. Include enrichments from process_mining.reference. Save to process_mining.silver.p2p_event_log.

### 3. What happens

Genie Code loads the skill and follows the same 4-phase workflow as Claude Code:

1. **Discover** — profiles tables using native UC metadata access
2. **Map & Test** — identifies case ID (`po_number`), maps timestamps to activities, tests with SQL
3. **Build** — UNION ALL + enrichment joins, validates quality
4. **Report** — saves to UC, prints summary

### 4. Expected result

Same output as Claude Code:
- ~27,000 events across ~5,000 cases
- 6 activities (Create PO → Approve PO → Goods Receipt → Invoice → Clear → Payment)
- Enriched with supplier and contract data

### 5. Compare with Claude Code

Run this SQL to verify both produced the same result:

```sql
SELECT 'Claude Code' AS source, COUNT(*) AS events, COUNT(DISTINCT case_id) AS cases
FROM process_mining.silver.p2p_event_log_claude
UNION ALL
SELECT 'Genie Code', COUNT(*), COUNT(DISTINCT case_id)
FROM process_mining.silver.p2p_event_log_genie
```
