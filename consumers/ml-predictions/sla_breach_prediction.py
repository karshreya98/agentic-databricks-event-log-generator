# Databricks notebook source
# MAGIC %md
# MAGIC # Predict SLA Breaches: Process + Operational Context
# MAGIC
# MAGIC Process mining tools offer native prediction capabilities that work well
# MAGIC with event log features — activity counts, rework loops, time elapsed.
# MAGIC
# MAGIC The lakehouse lets you go further: enrich process features with operational
# MAGIC context from other data in the catalog. Supplier risk ratings, contract
# MAGIC terms, inventory levels — signals that the event log alone doesn't carry.
# MAGIC
# MAGIC This notebook demonstrates both:
# MAGIC 1. **Process-only features** — what a process mining tool would see
# MAGIC 2. **Enriched features** — process + supplier + contract context
# MAGIC
# MAGIC **Input:** `process_mining.gold.case_summary` + supplier/contract data
# MAGIC **Output:** Registered MLflow model for SLA breach prediction

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Process Features (from the event log)
# MAGIC
# MAGIC These features come directly from the gold case summary — the same
# MAGIC information a process mining tool's native ML would use.

# COMMAND ----------

cases = spark.table("process_mining.gold.case_summary")

process_features = (
    cases
    .select(
        "case_id",
        "event_count",
        "distinct_activities",
        "distinct_resources",
        "total_cost",
        "has_rework",
        "source_system",
        "case_duration_days",
        F.col("sla_breach").cast("int").alias("label"),
    )
    .na.drop()
)

print(f"Total cases: {process_features.count():,}")
print(f"SLA breaches: {process_features.filter('label = 1').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Supplier & Contract Context
# MAGIC
# MAGIC In a real environment, this data already exists in the catalog —
# MAGIC supplier master from SAP, contract data from Ariba, etc.
# MAGIC Here we generate realistic reference data for the demo.

# COMMAND ----------

import random

# Generate supplier data
random.seed(42)
supplier_ids = [f"SUP-{i:04d}" for i in range(200)]
supplier_data = [
    {
        "supplier_id": sid,
        "credit_risk_rating": random.choice(["A", "A", "B", "B", "B", "C", "C", "D"]),
        "avg_delivery_lead_days": round(random.uniform(3, 45), 1),
        "open_quality_incidents": random.randint(0, 8),
        "on_time_delivery_rate": round(random.uniform(0.6, 0.99), 2),
        "years_as_supplier": random.randint(1, 20),
    }
    for sid in supplier_ids
]

suppliers = spark.createDataFrame(supplier_data)
suppliers.write.mode("overwrite").saveAsTable("process_mining.gold.supplier_master")

# Assign suppliers and contracts to cases
case_ids = [row.case_id for row in cases.select("case_id").collect()]
case_context = [
    {
        "case_id": cid,
        "supplier_id": random.choice(supplier_ids),
        "contract_amendment_count": random.choices([0, 0, 0, 1, 2, 3], weights=[40, 20, 10, 15, 10, 5])[0],
        "payment_terms_days": random.choice([15, 30, 30, 45, 60, 90]),
        "po_value": round(random.uniform(500, 250000), 2),
    }
    for cid in case_ids
]

case_refs = spark.createDataFrame(case_context)
case_refs.write.mode("overwrite").saveAsTable("process_mining.gold.case_references")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Enriched Features (process + operational context)
# MAGIC
# MAGIC This is where the lakehouse adds value: join process features
# MAGIC with supplier and contract data that lives alongside the event log.

# COMMAND ----------

enriched_features = (
    process_features
    .join(
        spark.table("process_mining.gold.case_references"),
        on="case_id",
        how="inner",
    )
    .join(
        spark.table("process_mining.gold.supplier_master"),
        on="supplier_id",
        how="inner",
    )
    .select(
        "case_id",
        # Process features
        "event_count",
        "distinct_activities",
        "distinct_resources",
        "total_cost",
        "has_rework",
        "source_system",
        # Supplier context
        "credit_risk_rating",
        "avg_delivery_lead_days",
        "open_quality_incidents",
        "on_time_delivery_rate",
        "years_as_supplier",
        # Contract context
        "contract_amendment_count",
        "payment_terms_days",
        "po_value",
        # Target
        "label",
    )
    .na.drop()
)

print(f"Enriched features: {enriched_features.count():,} cases, {len(enriched_features.columns)} columns")
display(enriched_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train — Process-Only vs. Enriched
# MAGIC
# MAGIC Compare both feature sets to see the impact of adding operational context.

# COMMAND ----------

from databricks import automl

# COMMAND ----------

# Model A: Process features only
print("=" * 60)
print("MODEL A: Process features only")
print("=" * 60)

summary_process = automl.classify(
    dataset=process_features.drop("case_id", "case_duration_days"),
    target_col="label",
    primary_metric="f1",
    timeout_minutes=10,
)

f1_process = summary_process.best_trial.metrics["test_f1_score"]
print(f"Best F1 (process only): {f1_process:.3f}")

# COMMAND ----------

# Model B: Enriched features (process + supplier + contract)
print("=" * 60)
print("MODEL B: Process + operational context")
print("=" * 60)

summary_enriched = automl.classify(
    dataset=enriched_features.drop("case_id"),
    target_col="label",
    primary_metric="f1",
    timeout_minutes=10,
)

f1_enriched = summary_enriched.best_trial.metrics["test_f1_score"]
print(f"Best F1 (enriched):     {f1_enriched:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Comparison

# COMMAND ----------

print(f"""
┌────────────────────────────────────────────────┐
│  SLA Breach Prediction — Feature Set Comparison │
├────────────────────────────────────────────────┤
│  Process features only:  F1 = {f1_process:.3f}              │
│  Process + operational:  F1 = {f1_enriched:.3f}              │
│                                                │
│  Improvement: {((f1_enriched - f1_process) / f1_process * 100):+.1f}%                            │
└────────────────────────────────────────────────┘

Process mining tools' native ML uses features like the first row.
The lakehouse lets you add the second row — and the improvement
comes from signals the event log alone doesn't carry.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Enriched Model

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

model_name = "process_mining.gold.sla_breach_predictor"
mlflow.register_model(
    f"runs:/{summary_enriched.best_trial.mlflow_run_id}/model",
    model_name,
)

print(f"Model registered: {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring In-Flight Cases
# MAGIC
# MAGIC In production, score cases that haven't completed yet to flag
# MAGIC likely SLA breaches before they happen.
# MAGIC
# MAGIC ```python
# MAGIC model = mlflow.pyfunc.load_model(f"models:/{model_name}/latest")
# MAGIC
# MAGIC # Build features from partial (in-flight) cases + supplier/contract data
# MAGIC in_flight_features = build_features(
# MAGIC     spark.table("process_mining.silver.event_log_in_flight")
# MAGIC )
# MAGIC predictions = model.predict(in_flight_features.toPandas())
# MAGIC ```
# MAGIC
# MAGIC The predictions can feed back into process mining tools via Delta Sharing,
# MAGIC power alerts in the AI/BI dashboard, or trigger Databricks Workflows
# MAGIC for proactive intervention.
