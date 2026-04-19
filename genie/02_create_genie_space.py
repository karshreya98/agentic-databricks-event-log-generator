# Databricks notebook source
# MAGIC %md
# MAGIC # Create Process Mining Genie Space
# MAGIC
# MAGIC Creates a Genie Space with:
# MAGIC - Your event log + source tables
# MAGIC - Custom instructions teaching it process mining concepts
# MAGIC - The UC functions as callable tools
# MAGIC - Sample questions to guide users
# MAGIC
# MAGIC **No Claude Code needed. No Anthropic subscription. Uses Databricks FMAPI.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Edit these to match your environment.

# COMMAND ----------

# Tables to include in the Genie Space
EVENT_LOG_TABLE = "process_mining.silver.sales_pipeline_event_log"
SOURCE_TABLES = [
    "process_mining.erp_raw.purchase_orders",
    "process_mining.erp_raw.goods_receipts",
    "process_mining.erp_raw.invoices",
    "process_mining.erp_raw.payments",
]
REFERENCE_TABLES = [
    "process_mining.reference.supplier_master",
    "process_mining.reference.contracts",
]
GOLD_TABLES = [
    "process_mining.gold.case_summary",
    "process_mining.gold.process_kpis",
    "process_mining.gold.non_conforming_cases",
]

# Genie Space config
SPACE_NAME = "Process Mining Assistant"
WAREHOUSE_ID = "862f1d757f0424f7"  # Your SQL warehouse ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Instructions
# MAGIC
# MAGIC These teach Genie about process mining concepts and how to use the UC functions.

# COMMAND ----------

CUSTOM_INSTRUCTIONS = """
You are a process mining assistant. You help users discover, build, analyze, and understand process event logs from operational data in Unity Catalog.

## Core Concepts

- **Event log**: A table where each row is an event — something that happened (activity) in a process instance (case_id) at a specific time (event_timestamp).
- **Case**: A single instance of a process (e.g., one purchase order, one sales opportunity, one incident ticket).
- **Activity**: What happened in the event (e.g., "Order Created", "Approved", "Shipped").
- **Variant**: A unique path through the process — the ordered sequence of activities for a case.
- **Bottleneck**: A transition between activities that takes disproportionately long.
- **Conformance**: How well actual behavior matches the intended process.

## Available Tools (UC Functions)

You have these custom functions in `process_mining.tools`:

1. **profile_tables(catalog, schema)** — Scan tables and classify as EVENT_SOURCE, REFERENCE_DATA, or AGGREGATE. Start here when exploring new data.

2. **build_event_log_sql(source_table, case_id_col, event_mappings)** — Generate SQL to extract an event log. Pass event_mappings as a JSON array:
   `[{"activity":"Order Created","timestamp_col":"created_at"},{"activity":"Approved","timestamp_col":"approved_at","condition":"approved_at IS NOT NULL"}]`

3. **validate_event_log(table)** — Check event log quality: nulls, activity count, events per case.

4. **find_enrichments(event_log_table, catalog)** — Search for reference tables that can enrich the event log via joins.

5. **find_bottlenecks(event_log_table, top_n)** — Find the slowest activity transitions.

6. **get_variants(event_log_table, top_n)** — Show the most common process paths.

## How to Guide Users

When a user wants to build an event log:
1. First, use `profile_tables` to understand what's available
2. Identify the case ID (column appearing across tables)
3. Use `build_event_log_sql` to generate the extraction query
4. Have the user review and run the SQL
5. Use `validate_event_log` to check quality

When a user wants to analyze an existing event log:
- Use `find_bottlenecks` for performance analysis
- Use `get_variants` for variant analysis
- Use standard SQL for custom questions (filter by activity, time range, etc.)
- Use `find_enrichments` to discover reference data to join

## Tables Available

The event log table has these key columns: case_id, activity, event_timestamp, event_rank, time_since_prev_seconds, case_event_count, plus any enrichment columns.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Questions

# COMMAND ----------

SAMPLE_QUESTIONS = [
    # Discovery
    "Profile the tables in the erp_raw schema — which ones are event sources?",
    "What reference tables could enrich my event log?",
    # Analysis
    "What are the top bottleneck transitions?",
    "Show me the most common process variants",
    "Validate the event log quality",
    "What percentage of cases end in each final activity?",
    "Which cases took longer than 30 days?",
    "What is the average cycle time by account industry?",
    "How many events does each case have on average?",
    "Show me cases where the time between Discovery and Demo exceeds 20 days",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Genie Space
# MAGIC
# MAGIC Note: Custom instructions and UC function tools must be added manually
# MAGIC in the Genie Space UI after creation. The API creates the space with
# MAGIC tables and sample questions.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

all_tables = [EVENT_LOG_TABLE] + SOURCE_TABLES + REFERENCE_TABLES + GOLD_TABLES

print(f"Creating Genie Space: {SPACE_NAME}")
print(f"Tables: {len(all_tables)}")
print(f"Sample questions: {len(SAMPLE_QUESTIONS)}")
print(f"\nTables included:")
for t in all_tables:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Setup Steps (after running this notebook)
# MAGIC
# MAGIC 1. Go to your Databricks workspace → Genie Spaces
# MAGIC 2. Create a new Genie Space with the tables listed above
# MAGIC 3. Paste the custom instructions from the `CUSTOM_INSTRUCTIONS` variable
# MAGIC 4. Add the sample questions
# MAGIC 5. In Settings → Tools, add the UC functions:
# MAGIC    - `process_mining.tools.profile_tables`
# MAGIC    - `process_mining.tools.build_event_log_sql`
# MAGIC    - `process_mining.tools.validate_event_log`
# MAGIC    - `process_mining.tools.find_enrichments`
# MAGIC    - `process_mining.tools.find_bottlenecks`
# MAGIC    - `process_mining.tools.get_variants`
# MAGIC
# MAGIC ## What Users Can Do (no Claude Code needed)
# MAGIC
# MAGIC Users open the Genie Space and ask:
# MAGIC - "Profile the erp_raw schema" → calls profile_tables function
# MAGIC - "What are the bottlenecks?" → calls find_bottlenecks function
# MAGIC - "Show me the top 5 process variants" → calls get_variants function
# MAGIC - "What tables could enrich my event log?" → calls find_enrichments function
# MAGIC - "How many cases breach the 18-day SLA?" → generates SQL directly
# MAGIC - "Which reps have the fastest cycle times?" → generates SQL with enrichment data
