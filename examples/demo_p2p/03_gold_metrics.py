# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Process KPIs, Case Features, Deviations
# MAGIC
# MAGIC The gold layer transforms the refined event log into consumable data products:
# MAGIC
# MAGIC | Table | Purpose | Consumers |
# MAGIC |-------|---------|-----------|
# MAGIC | `gold.case_summary` | One row per case with duration, variant, SLA flag | ML models, AI/BI dashboards |
# MAGIC | `gold.process_kpis` | Aggregate metrics by period | AI/BI dashboards, executive reporting |
# MAGIC | `gold.non_conforming_cases` | Cases deviating from the happy path | AI classification, root cause analysis |
# MAGIC
# MAGIC **Input:** `process_mining.silver.event_log`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table("process_mining.silver.event_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case Summary
# MAGIC One row per case — the foundation for ML features and dashboard KPIs.

# COMMAND ----------

HAPPY_PATH = [
    "Create Purchase Requisition",
    "Approve Purchase Requisition",
    "Create Purchase Order",
    "Approve Purchase Order",
    "Post Goods Receipt",
    "Receive Invoice",
    "Clear Invoice",
    "Process Payment",
]
HAPPY_PATH_STR = ",".join(HAPPY_PATH)

# SLA threshold: cases exceeding this many days are flagged
SLA_THRESHOLD_DAYS = 30

w = Window.partitionBy("case_id").orderBy("event_timestamp")

case_summary = (
    silver
    .groupBy("case_id")
    .agg(
        F.min("event_timestamp").alias("case_start"),
        F.max("event_timestamp").alias("case_end"),
        F.count("*").alias("event_count"),
        F.countDistinct("activity").alias("distinct_activities"),
        F.countDistinct("resource").alias("distinct_resources"),
        F.sum("cost").alias("total_cost"),
        F.first("department").alias("first_department"),
        F.first("source_system").alias("source_system"),
        # Build the variant (ordered activity sequence)
        F.concat_ws(",", F.collect_list(
            F.struct(F.col("event_rank"), F.col("activity"))
        ).cast("array<string>")).alias("_raw_variant"),
        # Collect ordered activities
        F.sort_array(F.collect_list(
            F.struct(F.col("event_rank").alias("rank"), F.col("activity"))
        )).alias("ordered_activities"),
    )
    .withColumn(
        "variant",
        F.concat_ws(",", F.transform(F.col("ordered_activities"), lambda x: x.activity))
    )
    .withColumn(
        "case_duration_seconds",
        F.col("case_end").cast("long") - F.col("case_start").cast("long")
    )
    .withColumn(
        "case_duration_days",
        F.col("case_duration_seconds") / 86400.0
    )
    .withColumn(
        "sla_breach",
        F.when(F.col("case_duration_days") > SLA_THRESHOLD_DAYS, True).otherwise(False)
    )
    .withColumn(
        "follows_happy_path",
        F.col("variant") == F.lit(HAPPY_PATH_STR)
    )
    .withColumn(
        "has_rework",
        F.col("event_count") > F.col("distinct_activities")
    )
    .drop("_raw_variant", "ordered_activities")
)

(
    case_summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("process_mining.gold.case_summary")
)

display(spark.table("process_mining.gold.case_summary").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process KPIs (aggregated by month)

# COMMAND ----------

kpis = (
    case_summary
    .withColumn("month", F.date_trunc("month", "case_start"))
    .groupBy("month")
    .agg(
        F.count("*").alias("cases_started"),
        F.avg("case_duration_days").alias("avg_duration_days"),
        F.expr("percentile(case_duration_days, 0.5)").alias("median_duration_days"),
        F.expr("percentile(case_duration_days, 0.95)").alias("p95_duration_days"),
        F.sum(F.when(F.col("sla_breach"), 1).otherwise(0)).alias("sla_breaches"),
        F.avg(F.when(F.col("follows_happy_path"), 1).otherwise(0)).alias("happy_path_rate"),
        F.avg(F.when(F.col("has_rework"), 1).otherwise(0)).alias("rework_rate"),
        F.avg("total_cost").alias("avg_case_cost"),
        F.countDistinct("variant").alias("unique_variants"),
    )
    .withColumn(
        "sla_breach_rate",
        F.col("sla_breaches") / F.col("cases_started")
    )
    .orderBy("month")
)

(
    kpis.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("process_mining.gold.process_kpis")
)

display(spark.table("process_mining.gold.process_kpis"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Non-Conforming Cases
# MAGIC
# MAGIC Cases that deviate from the happy path, with a description of how they deviate.
# MAGIC This table is designed to be consumed by `ai_classify` or `ai_query` for
# MAGIC automated root cause categorization.

# COMMAND ----------

non_conforming = (
    case_summary
    .filter(~F.col("follows_happy_path"))
    .withColumn(
        "deviation_description",
        F.concat(
            F.lit("P2P case "), F.col("case_id"),
            F.lit(" deviated from the standard path. "),
            F.lit("Duration: "), F.round("case_duration_days", 1), F.lit(" days. "),
            F.when(F.col("has_rework"), F.lit("Contains rework loops. ")).otherwise(F.lit("")),
            F.when(F.col("sla_breach"), F.lit("Breached SLA. ")).otherwise(F.lit("")),
            F.lit("Actual path: "), F.col("variant"),
        )
    )
    .select(
        "case_id", "variant", "case_duration_days", "sla_breach",
        "has_rework", "event_count", "total_cost", "source_system",
        "deviation_description",
    )
)

(
    non_conforming.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("process_mining.gold.non_conforming_cases")
)

count_nc = non_conforming.count()
count_total = case_summary.count()
print(f"Non-conforming cases: {count_nc:,} / {count_total:,} ({count_nc/count_total:.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Deviations with AI (optional)
# MAGIC
# MAGIC If Foundation Model APIs are available, classify deviations automatically:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     case_id,
# MAGIC     deviation_description,
# MAGIC     ai_classify(
# MAGIC         deviation_description,
# MAGIC         ARRAY('rework_loop', 'skipped_approval', 'sla_breach_only', 'abandoned_process')
# MAGIC     ) AS deviation_category
# MAGIC FROM process_mining.gold.non_conforming_cases
# MAGIC ```
