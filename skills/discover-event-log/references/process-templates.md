# Common Process Mining Patterns

Use these as a reference when reasoning about what kind of event log to build.

## Procure-to-Pay (P2P)

**Case ID:** Purchase Order number
**Typical flow:** PR Created → PR Approved → PO Created → PO Approved → Goods Receipt → Invoice Received → Invoice Cleared → Payment
**Source tables:** purchase_requisitions, purchase_orders, goods_receipts, invoices, payments
**Common enrichments:** supplier_master (credit risk, delivery rate), contracts (terms, amendments)
**Common issues:** Activity names vary across ERPs (SAP vs Oracle vs Ariba)

## Order-to-Cash (O2C)

**Case ID:** Sales Order ID
**Typical flow:** Order Created → Credit Check → Pick → Pack → Ship → Invoice → Payment Received
**Source tables:** sales_orders, credit_checks, shipments, invoices, payments
**Common enrichments:** customers (segment, lifetime value), inventory (stock level)
**Common issues:** Credit rejections create branching paths

## Incident Management (ITSM)

**Case ID:** Incident/Ticket ID
**Typical flow:** Created → Assigned → Investigation → Resolved → Closed
**Source tables:** incidents, escalations, reassignments, worknotes
**Common enrichments:** cmdb_ci (config item details), employees (team, location)
**Common issues:** Escalations and reassignments create loops

## Sales Pipeline / Lead-to-Close

**Case ID:** Opportunity ID
**Typical flow:** Lead → Qualified → Discovery → Demo → Proposal → Negotiation → Closed Won/Lost
**Source tables:** opportunities, opportunity_history, leads, activities
**Common enrichments:** accounts (industry, size), users (sales rep role)
**Common issues:** Often stored as snapshots (one row per opportunity) not event streams. Use multiple timestamp columns or stage history tables.

## Patient Journey (Healthcare)

**Case ID:** Patient Encounter ID
**Typical flow:** Registration → Triage → Examination → Diagnosis → Treatment → Discharge
**Source tables:** encounters, orders, results, medications, discharge_summaries
**Common enrichments:** patients (demographics), providers (specialty), facilities
**Common issues:** HIPAA — check for PII in resource/patient columns

## Data Patterns to Watch For

### Snapshot tables (one row per entity)
- Multiple timestamp columns → each is a separate event
- Status/stage column shows CURRENT state, not history
- Limited: you know when it started and ended, but not intermediate stages

### Event history tables (one row per event)
- Already event-like — map directly
- May need activity standardization if status values are codes

### Aggregate tables
- Low row count relative to other tables
- Dimensions instead of IDs (e.g., Region, Month)
- NOT useful for event logs — skip these

### CDC / audit tables
- One row per change with old/new values
- Rich event data but may be noisy
- Filter to meaningful state changes
