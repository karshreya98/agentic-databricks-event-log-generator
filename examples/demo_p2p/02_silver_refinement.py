# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Refine Raw Event Logs
# MAGIC
# MAGIC This is where Databricks earns its keep. Raw event logs are messy —
# MAGIC inconsistent activity names, timezone issues, cases spanning source systems.
# MAGIC Spark handles this at scale.
# MAGIC
# MAGIC **Input:** `process_mining.bronze.event_logs` (raw, messy)
# MAGIC **Output:** `process_mining.silver.event_log` (clean, analysis-ready)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

raw_events = spark.table("process_mining.bronze.event_logs")
print(f"Raw events: {raw_events.count():,}")
print(f"Distinct raw activity names: {raw_events.select('activity').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activity Standardization
# MAGIC
# MAGIC Source systems produce different names for the same activity.
# MAGIC We map them to canonical names. In production, this mapping lives in a
# MAGIC reference table — here we inline it for clarity.

# COMMAND ----------

# Canonical activity mapping
# In production, maintain this as a reference table in Unity Catalog
activity_mapping = {
    # Create Purchase Requisition
    "Create Purchase Requisition": "Create Purchase Requisition",
    "PR Created": "Create Purchase Requisition",
    "purchase_requisition_created": "Create Purchase Requisition",
    "Create PR": "Create Purchase Requisition",
    # Approve Purchase Requisition
    "Approve Purchase Requisition": "Approve Purchase Requisition",
    "PR Approved": "Approve Purchase Requisition",
    "Approve PR": "Approve Purchase Requisition",
    "purchase_req_approved": "Approve Purchase Requisition",
    # Create Purchase Order
    "Create Purchase Order": "Create Purchase Order",
    "PO Created": "Create Purchase Order",
    "CREATE_PO": "Create Purchase Order",
    "Create PO": "Create Purchase Order",
    # Approve Purchase Order
    "Approve Purchase Order": "Approve Purchase Order",
    "PO Approved": "Approve Purchase Order",
    "Approve PO": "Approve Purchase Order",
    "purchase_order_approved": "Approve Purchase Order",
    # Post Goods Receipt
    "Post Goods Receipt": "Post Goods Receipt",
    "GR Posted": "Post Goods Receipt",
    "Goods Receipt": "Post Goods Receipt",
    "goods_receipt_posted": "Post Goods Receipt",
    # Receive Invoice
    "Receive Invoice": "Receive Invoice",
    "Invoice Received": "Receive Invoice",
    "Inv Received": "Receive Invoice",
    "INVOICE_RECEIVED": "Receive Invoice",
    # Clear Invoice
    "Clear Invoice": "Clear Invoice",
    "Invoice Cleared": "Clear Invoice",
    "Clear Inv": "Clear Invoice",
    "invoice_cleared": "Clear Invoice",
    # Process Payment
    "Process Payment": "Process Payment",
    "Payment Sent": "Process Payment",
    "Paid": "Process Payment",
    "payment_processed": "Process Payment",
}

mapping_expr = F.create_map([F.lit(x) for pair in activity_mapping.items() for x in pair])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Silver Event Log

# COMMAND ----------

w = Window.partitionBy("case_id").orderBy("event_timestamp")

refined = (
    raw_events
    # Standardize activity names (unmapped activities pass through as-is)
    .withColumn(
        "activity",
        F.coalesce(mapping_expr[F.col("activity")], F.col("activity"))
    )
    # Normalize timestamps to UTC
    .withColumn(
        "event_timestamp",
        F.to_utc_timestamp(F.col("timestamp"), "America/New_York")
    )
    # Event ordering within each case
    .withColumn("event_rank", F.row_number().over(w))
    # Time since previous event in the same case (seconds)
    .withColumn(
        "time_since_prev_seconds",
        F.col("event_timestamp").cast("long")
        - F.lag("event_timestamp").over(w).cast("long")
    )
    # Total events per case (useful for filtering)
    .withColumn(
        "case_event_count",
        F.count("*").over(Window.partitionBy("case_id"))
    )
    .select(
        "event_id", "case_id", "activity", "event_timestamp",
        "resource", "cost", "department", "source_system",
        "event_rank", "time_since_prev_seconds", "case_event_count",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter to Complete Cases
# MAGIC
# MAGIC For process mining, we often want cases that have both a defined start
# MAGIC and end activity. Incomplete cases (still in progress or abandoned) can
# MAGIC be analyzed separately.

# COMMAND ----------

complete_cases = (
    refined
    .groupBy("case_id")
    .agg(F.collect_set("activity").alias("activities"))
    .filter(
        F.array_contains(F.col("activities"), "Create Purchase Requisition")
        & F.array_contains(F.col("activities"), "Process Payment")
    )
    .select("case_id")
)

silver_event_log = refined.join(complete_cases, on="case_id", how="inner")

# COMMAND ----------

(
    silver_event_log
    .write
    .mode("overwrite")
    .saveAsTable("process_mining.silver.event_log")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

silver = spark.table("process_mining.silver.event_log")

print(f"Silver events:    {silver.count():,}")
print(f"Complete cases:   {silver.select('case_id').distinct().count():,}")
print(f"Activities:       {silver.select('activity').distinct().count()}")

display(
    silver.groupBy("activity")
    .count()
    .orderBy(F.desc("count"))
)
