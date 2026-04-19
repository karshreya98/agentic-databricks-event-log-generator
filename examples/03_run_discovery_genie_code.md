# Example: Running Discovery with Genie Code

## Prerequisites

- Ran `01_create_source_tables.py` on a Databricks cluster
- Ran `./scripts/install-genie-code.sh`

## Run

Open Genie Code in your workspace (Agent mode):

> @discover-event-log Build both a traditional and OCEL event log from process_mining.erp_raw — it's a P2P process. Use process_mining.reference for enrichment.

## What to expect

Same as Claude Code — the skill logic is identical:

1. Profiles 8 tables, classifies as event sources / reference / enrichment
2. Discovers the many-to-many relationships (PR → POs → GRs → Invoices → Payments)
3. Builds traditional event log (single case_id = po_id)
4. Builds OCEL tables (events + objects + E2O with all object types)
5. Enriches with supplier, contract, cost center data
6. Validates quality gates

## Compare with Claude Code output

```sql
SELECT 'Claude Code' AS source, COUNT(*) AS events, COUNT(DISTINCT case_id) AS cases
FROM process_mining.silver.traditional_event_log_claude
UNION ALL
SELECT 'Genie Code', COUNT(*), COUNT(DISTINCT case_id)
FROM process_mining.silver.traditional_event_log_genie
```

Both should produce identical results — same skill, same data, different runtime.
