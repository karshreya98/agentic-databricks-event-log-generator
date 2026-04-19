# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Raw Event Logs
# MAGIC
# MAGIC This notebook sets up the bronze event log table and demonstrates
# MAGIC ingestion patterns. In production, events arrive continuously from
# MAGIC source systems via Auto Loader, Kafka, or CDC.
# MAGIC
# MAGIC **Output:** `process_mining.bronze.event_logs`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS process_mining;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS process_mining.bronze.event_logs (
# MAGIC   event_id       STRING,
# MAGIC   case_id        STRING,
# MAGIC   activity        STRING,
# MAGIC   timestamp       TIMESTAMP,
# MAGIC   resource        STRING,
# MAGIC   cost            DOUBLE,
# MAGIC   department      STRING,
# MAGIC   source_system   STRING,
# MAGIC   raw_payload     STRING,
# MAGIC   _ingested_at    TIMESTAMP DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Raw event logs from operational source systems. Activities are not standardized.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'bronze',
# MAGIC   'pipelines.autoOptimize.zOrderCols' = 'case_id'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Ingestion Patterns
# MAGIC
# MAGIC ### Auto Loader (files landing in cloud storage)
# MAGIC ```python
# MAGIC (
# MAGIC     spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", "/checkpoints/event_logs/schema")
# MAGIC     .load("/volumes/process_mining/landing/events/")
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", "/checkpoints/event_logs/")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable("process_mining.bronze.event_logs")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Kafka (real-time event stream)
# MAGIC ```python
# MAGIC (
# MAGIC     spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "<broker>")
# MAGIC     .option("subscribe", "erp-events,crm-events")
# MAGIC     .load()
# MAGIC     .selectExpr("CAST(value AS STRING) as raw_payload")
# MAGIC     .select(F.from_json("raw_payload", event_schema).alias("data"))
# MAGIC     .select("data.*")
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", "/checkpoints/event_logs_kafka/")
# MAGIC     .toTable("process_mining.bronze.event_logs")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Lakeflow Connect (CDC from SAP, Salesforce, etc.)
# MAGIC For change data capture from operational databases, use Lakeflow Connect
# MAGIC with the appropriate connector. CDC events map naturally to process events:
# MAGIC each INSERT/UPDATE on an order or requisition table is an activity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## For this demo: generate synthetic data
# MAGIC
# MAGIC Run `00_generate_sample_data.py` to populate the bronze table with
# MAGIC 5,000 synthetic Procure-to-Pay cases.
