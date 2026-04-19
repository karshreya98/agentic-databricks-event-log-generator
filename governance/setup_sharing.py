# Databricks notebook source
# MAGIC %md
# MAGIC # Governance & Delta Sharing Setup
# MAGIC
# MAGIC This notebook configures the process mining tables as governed data products
# MAGIC in Unity Catalog and sets up Delta Sharing for external consumers
# MAGIC (e.g., Celonis, Signavio, or partner organizations).
# MAGIC
# MAGIC ## What this enables
# MAGIC
# MAGIC ```
# MAGIC                        Unity Catalog
# MAGIC                     ┌──────────────────┐
# MAGIC                     │  process_mining   │
# MAGIC                     │  ├── bronze.*     │
# MAGIC                     │  ├── silver.*     │
# MAGIC   Internal ────────▶│  └── gold.*       │◀──────── Internal
# MAGIC   Consumers         │                    │          Governance
# MAGIC   (Apps, ML,        │   Delta Sharing    │          (Lineage,
# MAGIC    Dashboards)      │        │           │           Access Control)
# MAGIC                     └────────┼───────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC                     ┌──────────────────┐
# MAGIC                     │ External         │
# MAGIC                     │ Consumers        │
# MAGIC                     │ (Celonis, etc.)  │
# MAGIC                     └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Documentation
# MAGIC
# MAGIC Add descriptions so the event log is discoverable in the catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE process_mining.silver.event_log IS
# MAGIC   'Refined process event log with standardized activity names, UTC timestamps, and complete cases only. Source: SAP, Ariba, Coupa. Refresh: daily batch or streaming.';
# MAGIC
# MAGIC COMMENT ON TABLE process_mining.gold.case_summary IS
# MAGIC   'One row per process case with duration, variant, SLA breach flag, and cost. Derived from silver.event_log.';
# MAGIC
# MAGIC COMMENT ON TABLE process_mining.gold.process_kpis IS
# MAGIC   'Monthly aggregate process metrics: throughput, duration percentiles, SLA breach rate, rework rate, happy path rate.';
# MAGIC
# MAGIC COMMENT ON TABLE process_mining.gold.non_conforming_cases IS
# MAGIC   'Cases deviating from the standard P2P happy path with deviation descriptions. Designed for AI-powered root cause classification.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Tags
# MAGIC
# MAGIC Tag PII and business-critical columns for governance.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE process_mining.silver.event_log
# MAGIC   ALTER COLUMN resource SET TAGS ('pii' = 'true', 'contains' = 'employee_id');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Sharing: Share with External Tools
# MAGIC
# MAGIC Create a share for the silver and gold tables. External process mining tools
# MAGIC (Celonis, Signavio, etc.) connect via Delta Sharing — zero-copy, governed,
# MAGIC always up-to-date.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a share for process mining data
# MAGIC CREATE SHARE IF NOT EXISTS process_mining_share
# MAGIC   COMMENT 'Governed event log and process metrics for external process mining tools';
# MAGIC
# MAGIC -- Add the silver event log (the primary asset)
# MAGIC ALTER SHARE process_mining_share
# MAGIC   ADD TABLE process_mining.silver.event_log;
# MAGIC
# MAGIC -- Add gold tables for pre-computed metrics
# MAGIC ALTER SHARE process_mining_share
# MAGIC   ADD TABLE process_mining.gold.case_summary;
# MAGIC
# MAGIC ALTER SHARE process_mining_share
# MAGIC   ADD TABLE process_mining.gold.process_kpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Access to a Recipient
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a recipient (e.g., for Celonis)
# MAGIC CREATE RECIPIENT IF NOT EXISTS celonis_recipient
# MAGIC   COMMENT 'Celonis Process Intelligence platform';
# MAGIC
# MAGIC -- Grant access to the share
# MAGIC GRANT SELECT ON SHARE process_mining_share TO RECIPIENT celonis_recipient;
# MAGIC ```
# MAGIC
# MAGIC The recipient gets an activation link. Celonis uses this to connect
# MAGIC and read the shared tables directly — no data copy, no pipeline to maintain.
# MAGIC
# MAGIC ## Verify Sharing

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE process_mining_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage
# MAGIC
# MAGIC Unity Catalog automatically tracks lineage:
# MAGIC - `bronze.event_logs` → `silver.event_log` (refinement pipeline)
# MAGIC - `silver.event_log` → `gold.case_summary` (aggregation)
# MAGIC - `silver.event_log` → `gold.process_kpis` (aggregation)
# MAGIC - `silver.event_log` → `gold.non_conforming_cases` (filtering)
# MAGIC
# MAGIC This lineage is visible in the Catalog Explorer and answers questions like:
# MAGIC "What upstream sources feed the event log that Celonis is reading?"
