# Databricks notebook source
# MAGIC %md
# MAGIC # Register Process Mining UC Functions
# MAGIC
# MAGIC Creates Unity Catalog functions that Genie can call as custom tools.
# MAGIC These wrap the process mining logic so users can discover, build, and
# MAGIC validate event logs through natural language — no Claude Code needed.
# MAGIC
# MAGIC **Run this notebook once to register the functions. Then add them to a Genie Space.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS process_mining;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.tools;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 1: Profile Tables for Event-Like Patterns
# MAGIC
# MAGIC Scans a schema and classifies each table as event source, reference data,
# MAGIC aggregate, or irrelevant based on column types and cardinality.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.profile_tables(
# MAGIC   catalog_name STRING COMMENT 'The catalog to scan',
# MAGIC   schema_name STRING COMMENT 'The schema to scan'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   table_name STRING,
# MAGIC   row_count BIGINT,
# MAGIC   classification STRING,
# MAGIC   timestamp_columns STRING,
# MAGIC   id_columns STRING,
# MAGIC   status_columns STRING,
# MAGIC   reason STRING
# MAGIC )
# MAGIC COMMENT 'Profiles all tables in a schema and classifies them for process mining. Returns whether each table is an event source, reference data, aggregate, or irrelevant.'
# MAGIC RETURN
# MAGIC   WITH table_list AS (
# MAGIC     SELECT table_name
# MAGIC     FROM system.information_schema.tables
# MAGIC     WHERE table_catalog = catalog_name
# MAGIC       AND table_schema = schema_name
# MAGIC       AND table_type = 'MANAGED'
# MAGIC   ),
# MAGIC   column_info AS (
# MAGIC     SELECT
# MAGIC       c.table_name,
# MAGIC       COLLECT_LIST(CASE WHEN c.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')
# MAGIC                         OR c.column_name LIKE '%date%'
# MAGIC                         OR c.column_name LIKE '%_at'
# MAGIC                         OR c.column_name LIKE '%timestamp%'
# MAGIC                    THEN c.column_name END) AS ts_cols,
# MAGIC       COLLECT_LIST(CASE WHEN c.column_name LIKE '%id' OR c.column_name LIKE '%_id'
# MAGIC                         OR c.column_name IN ('id', 'key', 'number')
# MAGIC                    THEN c.column_name END) AS id_cols,
# MAGIC       COLLECT_LIST(CASE WHEN c.column_name LIKE '%status%'
# MAGIC                         OR c.column_name LIKE '%stage%'
# MAGIC                         OR c.column_name LIKE '%state%'
# MAGIC                         OR c.column_name LIKE '%activity%'
# MAGIC                    THEN c.column_name END) AS status_cols,
# MAGIC       COUNT(*) AS col_count
# MAGIC     FROM system.information_schema.columns c
# MAGIC     JOIN table_list t ON c.table_name = t.table_name
# MAGIC     WHERE c.table_catalog = catalog_name
# MAGIC       AND c.table_schema = schema_name
# MAGIC     GROUP BY c.table_name
# MAGIC   )
# MAGIC   SELECT
# MAGIC     CONCAT(catalog_name, '.', schema_name, '.', ci.table_name) AS table_name,
# MAGIC     0 AS row_count,
# MAGIC     CASE
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.ts_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.id_cols)) > 0
# MAGIC         THEN 'EVENT_SOURCE'
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.id_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.ts_cols)) = 0
# MAGIC         THEN 'REFERENCE_DATA'
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.ts_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.id_cols)) = 0
# MAGIC         THEN 'AGGREGATE'
# MAGIC       ELSE 'IRRELEVANT'
# MAGIC     END AS classification,
# MAGIC     ARRAY_JOIN(ARRAY_COMPACT(ci.ts_cols), ', ') AS timestamp_columns,
# MAGIC     ARRAY_JOIN(ARRAY_COMPACT(ci.id_cols), ', ') AS id_columns,
# MAGIC     ARRAY_JOIN(ARRAY_COMPACT(ci.status_cols), ', ') AS status_columns,
# MAGIC     CASE
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.ts_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.id_cols)) > 0
# MAGIC         THEN 'Has timestamp and ID columns — likely contains events'
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.id_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.ts_cols)) = 0
# MAGIC         THEN 'Has IDs but no timestamps — use for enrichment joins'
# MAGIC       WHEN SIZE(ARRAY_COMPACT(ci.ts_cols)) > 0 AND SIZE(ARRAY_COMPACT(ci.id_cols)) = 0
# MAGIC         THEN 'Has timestamps but no IDs — likely an aggregate/summary table'
# MAGIC       ELSE 'No timestamps or IDs found'
# MAGIC     END AS reason
# MAGIC   FROM column_info ci;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 2: Build Event Log from Sources
# MAGIC
# MAGIC Creates an event log table by extracting events from a source table.
# MAGIC Each timestamp column becomes an activity.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.build_event_log_sql(
# MAGIC   source_table STRING COMMENT 'Fully qualified source table name',
# MAGIC   case_id_col STRING COMMENT 'Column to use as case identifier',
# MAGIC   event_mappings STRING COMMENT 'JSON array of {activity, timestamp_col, condition} objects. Example: [{"activity":"Order Created","timestamp_col":"created_at"},{"activity":"Order Approved","timestamp_col":"approved_at","condition":"approved_at IS NOT NULL"}]'
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Generates a SQL query that extracts a process event log from a source table. Each event mapping defines an activity name and its timestamp column. Returns the SQL string — execute it to build the event log.'
# MAGIC RETURN
# MAGIC   CONCAT(
# MAGIC     'WITH raw_events AS (\n',
# MAGIC     ARRAY_JOIN(
# MAGIC       TRANSFORM(
# MAGIC         FROM_JSON(event_mappings, 'ARRAY<STRUCT<activity: STRING, timestamp_col: STRING, condition: STRING>>'),
# MAGIC         x -> CONCAT(
# MAGIC           '  SELECT ', case_id_col, ' AS case_id, ',
# MAGIC           '''', x.activity, ''' AS activity, ',
# MAGIC           'CAST(', x.timestamp_col, ' AS TIMESTAMP) AS event_timestamp ',
# MAGIC           'FROM ', source_table,
# MAGIC           CASE WHEN x.condition IS NOT NULL
# MAGIC                THEN CONCAT(' WHERE ', x.condition)
# MAGIC                ELSE '' END
# MAGIC         )
# MAGIC       ),
# MAGIC       '\n  UNION ALL\n'
# MAGIC     ),
# MAGIC     '\n)\n',
# MAGIC     'SELECT case_id, activity, event_timestamp,\n',
# MAGIC     '  ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY event_timestamp) AS event_rank,\n',
# MAGIC     '  UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(\n',
# MAGIC     '    LAG(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp)\n',
# MAGIC     '  ) AS time_since_prev_seconds,\n',
# MAGIC     '  COUNT(*) OVER (PARTITION BY case_id) AS case_event_count\n',
# MAGIC     'FROM raw_events\n',
# MAGIC     'WHERE event_timestamp IS NOT NULL\n',
# MAGIC     'ORDER BY case_id, event_timestamp'
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 3: Validate Event Log Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.validate_event_log(
# MAGIC   event_log_table STRING COMMENT 'Fully qualified table name of the event log to validate'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   check_name STRING,
# MAGIC   status STRING,
# MAGIC   detail STRING
# MAGIC )
# MAGIC COMMENT 'Validates an event log table for process mining readiness. Checks for null values, activity count, events per case, and timestamp ordering. Returns pass/warn/fail for each check.'
# MAGIC RETURN
# MAGIC   WITH stats AS (
# MAGIC     SELECT
# MAGIC       COUNT(*) AS total_events,
# MAGIC       COUNT(DISTINCT case_id) AS total_cases,
# MAGIC       COUNT(DISTINCT activity) AS total_activities,
# MAGIC       SUM(CASE WHEN case_id IS NULL THEN 1 ELSE 0 END) AS null_case_ids,
# MAGIC       SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
# MAGIC       AVG(case_event_count) AS avg_events_per_case,
# MAGIC       MIN(case_event_count) AS min_events_per_case,
# MAGIC       MAX(case_event_count) AS max_events_per_case
# MAGIC     FROM IDENTIFIER(event_log_table)
# MAGIC   )
# MAGIC   SELECT 'total_events' AS check_name,
# MAGIC          CASE WHEN total_events > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC          CONCAT(total_events, ' events across ', total_cases, ' cases') AS detail
# MAGIC   FROM stats
# MAGIC   UNION ALL
# MAGIC   SELECT 'null_case_ids',
# MAGIC          CASE WHEN null_case_ids = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC          CONCAT(null_case_ids, ' null case IDs')
# MAGIC   FROM stats
# MAGIC   UNION ALL
# MAGIC   SELECT 'null_timestamps',
# MAGIC          CASE WHEN null_timestamps = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC          CONCAT(null_timestamps, ' null timestamps')
# MAGIC   FROM stats
# MAGIC   UNION ALL
# MAGIC   SELECT 'activity_count',
# MAGIC          CASE WHEN total_activities >= 2 THEN 'PASS'
# MAGIC               WHEN total_activities = 1 THEN 'WARN'
# MAGIC               ELSE 'FAIL' END,
# MAGIC          CONCAT(total_activities, ' distinct activities')
# MAGIC   FROM stats
# MAGIC   UNION ALL
# MAGIC   SELECT 'events_per_case',
# MAGIC          CASE WHEN avg_events_per_case > 1 THEN 'PASS' ELSE 'WARN' END,
# MAGIC          CONCAT('min=', min_events_per_case, ' avg=', ROUND(avg_events_per_case, 1), ' max=', max_events_per_case)
# MAGIC   FROM stats;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 4: Find Enrichment Candidates

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.find_enrichments(
# MAGIC   event_log_table STRING COMMENT 'Fully qualified event log table name',
# MAGIC   search_catalog STRING COMMENT 'Catalog to search for enrichment tables'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   enrichment_table STRING,
# MAGIC   join_key STRING,
# MAGIC   available_columns STRING,
# MAGIC   recommendation STRING
# MAGIC )
# MAGIC COMMENT 'Searches a catalog for reference tables that can enrich the event log. Finds tables with columns matching the event log foreign keys (e.g., account_id, supplier_id) and lists the columns they would add.'
# MAGIC RETURN
# MAGIC   WITH event_log_cols AS (
# MAGIC     SELECT column_name
# MAGIC     FROM system.information_schema.columns
# MAGIC     WHERE CONCAT(table_catalog, '.', table_schema, '.', table_name) = event_log_table
# MAGIC       AND (column_name LIKE '%_id' OR column_name LIKE '%id')
# MAGIC       AND column_name NOT IN ('case_id', 'event_id')
# MAGIC   ),
# MAGIC   candidate_tables AS (
# MAGIC     SELECT
# MAGIC       CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) AS full_table_name,
# MAGIC       c.column_name AS matching_col,
# MAGIC       elc.column_name AS event_log_col
# MAGIC     FROM system.information_schema.columns c
# MAGIC     JOIN event_log_cols elc
# MAGIC       ON (c.column_name = elc.column_name OR c.column_name = 'id')
# MAGIC     WHERE c.table_catalog = search_catalog
# MAGIC       AND CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name) != event_log_table
# MAGIC   ),
# MAGIC   enrichment_cols AS (
# MAGIC     SELECT
# MAGIC       ct.full_table_name,
# MAGIC       ct.event_log_col AS join_key,
# MAGIC       ARRAY_JOIN(
# MAGIC         COLLECT_LIST(
# MAGIC           CASE WHEN c2.column_name != ct.matching_col
# MAGIC                 AND c2.column_name NOT LIKE '%_id'
# MAGIC                 AND c2.column_name != 'id'
# MAGIC                THEN c2.column_name END
# MAGIC         ), ', '
# MAGIC       ) AS available_columns
# MAGIC     FROM candidate_tables ct
# MAGIC     JOIN system.information_schema.columns c2
# MAGIC       ON CONCAT(c2.table_catalog, '.', c2.table_schema, '.', c2.table_name) = ct.full_table_name
# MAGIC     WHERE c2.table_catalog = search_catalog
# MAGIC     GROUP BY ct.full_table_name, ct.event_log_col
# MAGIC   )
# MAGIC   SELECT
# MAGIC     full_table_name AS enrichment_table,
# MAGIC     join_key,
# MAGIC     available_columns,
# MAGIC     CONCAT('JOIN ', full_table_name, ' ON event_log.', join_key, ' = enrichment.',
# MAGIC            CASE WHEN join_key = 'id' THEN join_key ELSE 'id' END) AS recommendation
# MAGIC   FROM enrichment_cols
# MAGIC   WHERE available_columns IS NOT NULL AND available_columns != '';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 5: Bottleneck Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.find_bottlenecks(
# MAGIC   event_log_table STRING COMMENT 'Fully qualified event log table name',
# MAGIC   top_n INT DEFAULT 10 COMMENT 'Number of top bottleneck transitions to return'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   from_activity STRING,
# MAGIC   to_activity STRING,
# MAGIC   median_hours DOUBLE,
# MAGIC   transition_count BIGINT
# MAGIC )
# MAGIC COMMENT 'Finds the slowest transitions in the process — which activity-to-activity steps take the longest. Returns the top N bottleneck transitions ranked by median time.'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC     from_activity,
# MAGIC     to_activity,
# MAGIC     ROUND(PERCENTILE(time_seconds / 3600.0, 0.5), 1) AS median_hours,
# MAGIC     COUNT(*) AS transition_count
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       activity AS from_activity,
# MAGIC       LEAD(activity) OVER (PARTITION BY case_id ORDER BY event_timestamp) AS to_activity,
# MAGIC       UNIX_TIMESTAMP(LEAD(event_timestamp) OVER (PARTITION BY case_id ORDER BY event_timestamp))
# MAGIC         - UNIX_TIMESTAMP(event_timestamp) AS time_seconds
# MAGIC     FROM IDENTIFIER(event_log_table)
# MAGIC   )
# MAGIC   WHERE to_activity IS NOT NULL
# MAGIC   GROUP BY from_activity, to_activity
# MAGIC   ORDER BY median_hours DESC
# MAGIC   LIMIT top_n;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function 6: Variant Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION process_mining.tools.get_variants(
# MAGIC   event_log_table STRING COMMENT 'Fully qualified event log table name',
# MAGIC   top_n INT DEFAULT 10 COMMENT 'Number of top variants to return'
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   variant_path STRING,
# MAGIC   case_count BIGINT,
# MAGIC   pct_of_total DOUBLE,
# MAGIC   avg_duration_days DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns the most common process variants (unique paths through the process). Shows how many cases follow each path and their average duration.'
# MAGIC RETURN
# MAGIC   WITH case_variants AS (
# MAGIC     SELECT
# MAGIC       case_id,
# MAGIC       CONCAT_WS(' → ', SORT_ARRAY(COLLECT_LIST(STRUCT(event_rank, activity))).activity) AS variant_path,
# MAGIC       (UNIX_TIMESTAMP(MAX(event_timestamp)) - UNIX_TIMESTAMP(MIN(event_timestamp))) / 86400.0 AS duration_days
# MAGIC     FROM IDENTIFIER(event_log_table)
# MAGIC     GROUP BY case_id
# MAGIC   ),
# MAGIC   total AS (
# MAGIC     SELECT COUNT(*) AS total_cases FROM case_variants
# MAGIC   )
# MAGIC   SELECT
# MAGIC     variant_path,
# MAGIC     COUNT(*) AS case_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / MAX(t.total_cases), 1) AS pct_of_total,
# MAGIC     ROUND(AVG(duration_days), 1) AS avg_duration_days
# MAGIC   FROM case_variants, total t
# MAGIC   GROUP BY variant_path
# MAGIC   ORDER BY case_count DESC
# MAGIC   LIMIT top_n;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all registered process mining functions
# MAGIC SHOW FUNCTIONS IN process_mining.tools;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC These functions are now registered in Unity Catalog. Add them to a Genie Space:
# MAGIC
# MAGIC 1. Create a Genie Space with your source tables + event log tables
# MAGIC 2. In the Genie Space settings, add these functions as tools
# MAGIC 3. Users can now ask natural language questions like:
# MAGIC    - "Profile the tables in the erp_raw schema"
# MAGIC    - "What are the bottleneck transitions?"
# MAGIC    - "Show me the top process variants"
# MAGIC    - "Validate the event log quality"
# MAGIC    - "What reference tables could enrich my event log?"
