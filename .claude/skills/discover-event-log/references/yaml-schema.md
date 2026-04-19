# Event Log YAML Config Schema

## Full Schema

```yaml
event_log:
  # Required
  name: string              # Human-readable process name
  output_table: string      # Fully qualified UC table (catalog.schema.table)
  sources: list             # One or more source table definitions

  # Optional
  standardization:
    activity_overrides:     # Map raw activity names to canonical names
      "raw_name": "Canonical Name"

  enrichment: list          # Reference tables to join
```

## Source Definition

```yaml
sources:
  - table: string           # Fully qualified table name
    case_id: string         # Column that identifies the process instance
    timezone: string        # Optional: source timezone (default: UTC)
    events: list            # One or more event definitions
```

## Event Definition

```yaml
events:
  - activity: string        # Human-readable activity name
    timestamp: string       # Column containing the event timestamp
    condition: string       # Optional: SQL WHERE clause (e.g., "approved_at IS NOT NULL")
    resource: string        # Optional: column for who/what performed the activity
    cost: string            # Optional: column for cost associated with the event
```

## Enrichment Definition

```yaml
enrichment:
  - table: string           # Fully qualified reference table
    join_key: string        # Column to join on (must exist in both event log and reference table)
    columns: list           # Columns to add from the reference table
```

## Rules

1. `case_id` must be a column that groups related events into a single process instance
2. Each event must have an `activity` name and `timestamp` column
3. If a timestamp column can be NULL, add a `condition` to filter nulls
4. Activity names should be human-readable ("Create Purchase Order" not "PO_CREATED")
5. Enrichment `join_key` must exist in the event log (possibly via a foreign key from a source table)
6. Enrichment `columns` should not duplicate columns already in the event log

## Example

```yaml
event_log:
  name: procure_to_pay
  output_table: process_mining.silver.p2p_event_log
  sources:
    - table: erp.procurement.purchase_orders
      case_id: po_number
      events:
        - activity: "Create Purchase Order"
          timestamp: created_at
          resource: buyer
          cost: po_value
        - activity: "Approve Purchase Order"
          timestamp: approved_at
          condition: "approved_at IS NOT NULL"
          resource: approver
    - table: erp.warehouse.goods_receipts
      case_id: po_number
      events:
        - activity: "Post Goods Receipt"
          timestamp: posting_date
          resource: receiver
  enrichment:
    - table: erp.procurement.supplier_master
      join_key: supplier_id
      columns: [credit_risk_rating, on_time_delivery_rate]
```
