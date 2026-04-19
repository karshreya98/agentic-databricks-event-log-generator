# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create Source Tables
# MAGIC
# MAGIC Generates realistic ERP-like source tables in Unity Catalog — the kind
# MAGIC of data a customer already has before process mining.
# MAGIC
# MAGIC These are NOT event logs. They're operational tables:
# MAGIC - `purchase_orders` — one row per PO with created/approved timestamps
# MAGIC - `goods_receipts` — one row per goods receipt
# MAGIC - `invoices` — one row per invoice with received/cleared dates
# MAGIC - `payments` — one row per payment
# MAGIC - `supplier_master` — reference data (credit risk, delivery rate)
# MAGIC - `contracts` — reference data (terms, amendments)
# MAGIC
# MAGIC The agentic skill's job is to figure out that these tables contain
# MAGIC a P2P process and build an event log from them.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS process_mining;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.erp_raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.reference;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.silver;

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

random.seed(42)

# ── P2P process definition ──
ACTIVITIES = [
    "Create Purchase Requisition",
    "Approve Purchase Requisition",
    "Create Purchase Order",
    "Approve Purchase Order",
    "Post Goods Receipt",
    "Receive Invoice",
    "Clear Invoice",
    "Process Payment",
]

DEPARTMENTS = ["Procurement", "Finance", "Warehouse", "Accounts Payable", "Management"]
RESOURCES = ["alice.jones", "bob.smith", "carol.wu", "dave.patel", "emma.garcia",
             "frank.kim", "grace.lee", "henry.chen", "iris.taylor", "jack.wilson"]
SOURCE_SYSTEMS = ["SAP_ECC", "SAP_S4", "Ariba", "Coupa"]

# ── Generate 5,000 P2P cases ──
cases = []
for i in range(5000):
    case_id = f"P2P-2024-{i+1:05d}"
    start = datetime(2024, 1, 1) + timedelta(hours=random.uniform(0, 180 * 24))
    supplier_id = f"SUP-{random.randint(0, 199):04d}"
    contract_id = f"CTR-{random.randint(0, 79):04d}"
    source = random.choice(SOURCE_SYSTEMS)
    po_value = round(random.uniform(500, 250000), 2)

    # Decide case profile
    profile = random.choices(["happy", "rework", "partial", "fast"],
                              weights=[0.50, 0.25, 0.10, 0.15])[0]

    timestamps = {}
    t = start
    for act in ACTIVITIES:
        if profile == "partial" and act in ("Clear Invoice", "Process Payment"):
            break
        if profile == "fast" and "Approve" in act:
            continue
        delay = random.uniform(4, 120) if "Approve" in act else (
                random.uniform(24, 240) if "Payment" in act else random.uniform(1, 48))
        t += timedelta(hours=delay)
        timestamps[act] = t

    cases.append({
        "case_id": case_id,
        "supplier_id": supplier_id,
        "contract_id": contract_id,
        "source_system": source,
        "po_value": po_value,
        "timestamps": timestamps,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Purchase Orders Table

# COMMAND ----------

po_data = [{
    "po_number": c["case_id"],
    "created_at": c["timestamps"].get("Create Purchase Order"),
    "approved_at": c["timestamps"].get("Approve Purchase Order"),
    "buyer": random.choice(RESOURCES),
    "approver": random.choice(RESOURCES),
    "po_value": c["po_value"],
    "supplier_id": c["supplier_id"],
    "contract_id": c["contract_id"],
    "source_system": c["source_system"],
    "status": "approved" if "Approve Purchase Order" in c["timestamps"] else "pending",
} for c in cases if "Create Purchase Order" in c["timestamps"]]

spark.createDataFrame(po_data).write.mode("overwrite").saveAsTable("process_mining.erp_raw.purchase_orders")
print(f"purchase_orders: {len(po_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Goods Receipts Table

# COMMAND ----------

gr_data = [{
    "po_number": c["case_id"],
    "posting_date": c["timestamps"]["Post Goods Receipt"],
    "receiver": random.choice(RESOURCES),
    "warehouse": random.choice(["WH-East", "WH-West", "WH-Central"]),
} for c in cases if "Post Goods Receipt" in c["timestamps"]]

spark.createDataFrame(gr_data).write.mode("overwrite").saveAsTable("process_mining.erp_raw.goods_receipts")
print(f"goods_receipts: {len(gr_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Invoices Table

# COMMAND ----------

inv_data = [{
    "po_number": c["case_id"],
    "invoice_id": f"INV-{i+1:06d}",
    "received_date": c["timestamps"].get("Receive Invoice"),
    "cleared_date": c["timestamps"].get("Clear Invoice"),
    "invoice_amount": round(c["po_value"] * random.uniform(0.95, 1.05), 2),
} for i, c in enumerate(cases) if "Receive Invoice" in c["timestamps"]]

spark.createDataFrame(inv_data).write.mode("overwrite").saveAsTable("process_mining.erp_raw.invoices")
print(f"invoices: {len(inv_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Payments Table

# COMMAND ----------

pay_data = [{
    "po_number": c["case_id"],
    "payment_date": c["timestamps"]["Process Payment"],
    "payment_amount": round(c["po_value"] * random.uniform(0.98, 1.02), 2),
    "processed_by": random.choice(RESOURCES),
} for c in cases if "Process Payment" in c["timestamps"]]

spark.createDataFrame(pay_data).write.mode("overwrite").saveAsTable("process_mining.erp_raw.payments")
print(f"payments: {len(pay_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Supplier Master (Reference Data)

# COMMAND ----------

supplier_data = [{
    "supplier_id": f"SUP-{i:04d}",
    "supplier_name": f"Supplier {i}",
    "credit_risk_rating": random.choice(["A", "A", "B", "B", "B", "C", "C", "D"]),
    "avg_delivery_lead_days": round(random.uniform(3, 45), 1),
    "open_quality_incidents": random.randint(0, 8),
    "on_time_delivery_rate": round(random.uniform(0.6, 0.99), 2),
    "country": random.choice(["US", "DE", "CN", "IN", "JP", "MX", "BR"]),
} for i in range(200)]

spark.createDataFrame(supplier_data).write.mode("overwrite").saveAsTable("process_mining.reference.supplier_master")
print(f"supplier_master: {len(supplier_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Contracts (Reference Data)

# COMMAND ----------

contract_data = [{
    "contract_id": f"CTR-{i:04d}",
    "contract_type": random.choice(["Fixed Price", "Time & Materials", "Framework", "Blanket"]),
    "amendment_count": random.choices([0, 0, 0, 1, 2, 3], weights=[40, 20, 10, 15, 10, 5])[0],
    "payment_terms_days": random.choice([15, 30, 30, 45, 60, 90]),
} for i in range(80)]

spark.createDataFrame(contract_data).write.mode("overwrite").saveAsTable("process_mining.reference.contracts")
print(f"contracts: {len(contract_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Tables created:
# MAGIC - `process_mining.erp_raw.purchase_orders`
# MAGIC - `process_mining.erp_raw.goods_receipts`
# MAGIC - `process_mining.erp_raw.invoices`
# MAGIC - `process_mining.erp_raw.payments`
# MAGIC - `process_mining.reference.supplier_master`
# MAGIC - `process_mining.reference.contracts`
# MAGIC
# MAGIC **Next:** Run the agentic skill against these tables:
# MAGIC
# MAGIC **Claude Code:**
# MAGIC ```
# MAGIC /discover-event-log
# MAGIC "Build an event log from process_mining.erp_raw — it's a procure-to-pay process"
# MAGIC ```
# MAGIC
# MAGIC **Genie Code:**
# MAGIC ```
# MAGIC @discover-event-log Build an event log from process_mining.erp_raw — it's a P2P process
# MAGIC ```
