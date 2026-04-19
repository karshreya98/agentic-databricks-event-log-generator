# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Create Realistic Source Tables
# MAGIC
# MAGIC Generates ERP-like source tables that reflect real-world complexity:
# MAGIC
# MAGIC **Operational tables (event sources):**
# MAGIC - `purchase_requisitions` — one row per PR with created/approved timestamps
# MAGIC - `purchase_orders` — one row per PO (a PR can spawn multiple POs)
# MAGIC - `goods_receipts` — one row per GR (a PO can have multiple partial receipts)
# MAGIC - `invoices` — one row per invoice (a PO can have multiple invoices)
# MAGIC - `payments` — one row per payment (an invoice can have partial payments)
# MAGIC
# MAGIC **Reference tables (enrichment):**
# MAGIC - `supplier_master` — credit risk, delivery rates, country
# MAGIC - `contracts` — contract type, terms, amendments
# MAGIC - `cost_centers` — department, business unit, region
# MAGIC
# MAGIC **Why this matters for OCEL:**
# MAGIC - One PR → 1-3 POs (split by category)
# MAGIC - One PO → 1-3 goods receipts (partial deliveries)
# MAGIC - One PO → 1-2 invoices (split billing)
# MAGIC - These many-to-many relationships are lost in traditional PM (single case_id)
# MAGIC - OCEL preserves them: each event links to PR + PO + Invoice + Supplier + Contract

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

random.seed(42)

RESOURCES = ["alice.jones", "bob.smith", "carol.wu", "dave.patel", "emma.garcia",
             "frank.kim", "grace.lee", "henry.chen", "iris.taylor", "jack.wilson"]
APPROVERS = ["mgr.williams", "mgr.johnson", "mgr.brown", "mgr.davis", "dir.wilson"]
SYSTEMS = ["SAP_ECC", "SAP_S4", "Ariba", "Coupa"]
CATEGORIES = ["IT Equipment", "Office Supplies", "Raw Materials", "Services", "MRO"]
DEPARTMENTS = ["Procurement", "Finance", "Engineering", "Operations", "Sales"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the Process Data
# MAGIC
# MAGIC We generate from the top down: PRs → POs → GRs → Invoices → Payments.
# MAGIC Each step introduces realistic many-to-many relationships.

# COMMAND ----------

prs = []         # purchase requisitions
pos = []         # purchase orders
grs = []         # goods receipts
invoices = []    # invoices
payments = []    # payments

for i in range(2000):
    pr_id = f"PR-{i+1:05d}"
    pr_date = datetime(2024, 1, 1) + timedelta(hours=random.uniform(0, 180 * 24))
    requestor = random.choice(RESOURCES)
    department = random.choice(DEPARTMENTS)
    cost_center = f"CC-{random.randint(100, 199)}"
    supplier_id = f"SUP-{random.randint(0, 149):04d}"
    contract_id = f"CTR-{random.randint(0, 59):04d}"
    system = random.choice(SYSTEMS)

    # PR approval (90% get approved)
    approved = random.random() < 0.9
    approve_date = pr_date + timedelta(hours=random.uniform(4, 96)) if approved else None
    approver = random.choice(APPROVERS) if approved else None

    prs.append({
        "pr_id": pr_id,
        "created_at": pr_date,
        "approved_at": approve_date,
        "requestor": requestor,
        "approver": approver,
        "department": department,
        "cost_center": cost_center,
        "category": random.choice(CATEGORIES),
        "total_value": round(random.uniform(500, 100000), 2),
        "source_system": system,
        "status": "approved" if approved else "pending",
    })

    if not approved:
        continue

    # Each PR generates 1-3 POs (split by category or vendor)
    num_pos = random.choices([1, 1, 1, 2, 2, 3], weights=[40, 20, 10, 15, 10, 5])[0]
    for j in range(num_pos):
        po_id = f"PO-{i+1:05d}-{j+1}"
        po_date = approve_date + timedelta(hours=random.uniform(1, 48))
        po_value = round(random.uniform(500, 50000), 2)

        # PO approval (95% get approved)
        po_approved = random.random() < 0.95
        po_approve_date = po_date + timedelta(hours=random.uniform(4, 120)) if po_approved else None

        pos.append({
            "po_id": po_id,
            "pr_id": pr_id,
            "supplier_id": supplier_id,
            "contract_id": contract_id,
            "created_at": po_date,
            "approved_at": po_approve_date,
            "buyer": random.choice(RESOURCES),
            "approver": random.choice(APPROVERS) if po_approved else None,
            "po_value": po_value,
            "source_system": system,
            "status": "approved" if po_approved else "pending",
        })

        if not po_approved:
            continue

        # Each PO gets 1-3 goods receipts (partial deliveries)
        num_grs = random.choices([1, 1, 2, 3], weights=[60, 15, 15, 10])[0]
        for k in range(num_grs):
            gr_date = po_approve_date + timedelta(hours=random.uniform(24, 240))
            gr_id = f"GR-{po_id}-{k+1}"
            grs.append({
                "gr_id": gr_id,
                "po_id": po_id,
                "posting_date": gr_date,
                "receiver": random.choice(RESOURCES),
                "warehouse": random.choice(["WH-East", "WH-West", "WH-Central"]),
                "quantity_received": round(po_value / num_grs * random.uniform(0.8, 1.0), 2),
            })

        # Each PO gets 1-2 invoices (split billing or corrections)
        num_inv = random.choices([1, 1, 2], weights=[70, 15, 15])[0]
        for k in range(num_inv):
            inv_date = po_approve_date + timedelta(hours=random.uniform(48, 360))
            inv_id = f"INV-{po_id}-{k+1}"
            clear_date = inv_date + timedelta(hours=random.uniform(24, 168)) if random.random() < 0.85 else None
            inv_amount = round(po_value / num_inv * random.uniform(0.95, 1.05), 2)

            invoices.append({
                "invoice_id": inv_id,
                "po_id": po_id,
                "supplier_id": supplier_id,
                "received_date": inv_date,
                "cleared_date": clear_date,
                "invoice_amount": inv_amount,
                "three_way_match": random.choice(["pass", "pass", "pass", "fail"]),
            })

            # Payment only if invoice is cleared
            if clear_date:
                pay_date = clear_date + timedelta(hours=random.uniform(24, 240))
                payments.append({
                    "payment_id": f"PAY-{inv_id}",
                    "invoice_id": inv_id,
                    "po_id": po_id,
                    "supplier_id": supplier_id,
                    "payment_date": pay_date,
                    "payment_amount": inv_amount,
                    "payment_method": random.choice(["Wire", "ACH", "Check"]),
                    "processed_by": random.choice(RESOURCES),
                })

print(f"Generated:")
print(f"  PRs:       {len(prs):,}")
print(f"  POs:       {len(pos):,}")
print(f"  GRs:       {len(grs):,}")
print(f"  Invoices:  {len(invoices):,}")
print(f"  Payments:  {len(payments):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Operational Tables

# COMMAND ----------

spark.createDataFrame(prs).write.mode("overwrite").saveAsTable("process_mining.erp_raw.purchase_requisitions")
spark.createDataFrame(pos).write.mode("overwrite").saveAsTable("process_mining.erp_raw.purchase_orders")
spark.createDataFrame(grs).write.mode("overwrite").saveAsTable("process_mining.erp_raw.goods_receipts")
spark.createDataFrame(invoices).write.mode("overwrite").saveAsTable("process_mining.erp_raw.invoices")
spark.createDataFrame(payments).write.mode("overwrite").saveAsTable("process_mining.erp_raw.payments")

print("Operational tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Reference Tables

# COMMAND ----------

suppliers = [{
    "supplier_id": f"SUP-{i:04d}",
    "supplier_name": f"Supplier {i}",
    "credit_risk_rating": random.choice(["A", "A", "B", "B", "B", "C", "C", "D"]),
    "avg_delivery_lead_days": round(random.uniform(3, 45), 1),
    "open_quality_incidents": random.randint(0, 8),
    "on_time_delivery_rate": round(random.uniform(0.6, 0.99), 2),
    "country": random.choice(["US", "DE", "CN", "IN", "JP", "MX", "BR"]),
} for i in range(150)]

contracts = [{
    "contract_id": f"CTR-{i:04d}",
    "contract_type": random.choice(["Fixed Price", "Time & Materials", "Framework", "Blanket"]),
    "amendment_count": random.choices([0, 0, 0, 1, 2, 3], weights=[40, 20, 10, 15, 10, 5])[0],
    "payment_terms_days": random.choice([15, 30, 30, 45, 60, 90]),
    "max_value_usd": round(random.uniform(50000, 5000000), 2),
} for i in range(60)]

cost_centers = [{
    "cost_center": f"CC-{i}",
    "department": random.choice(DEPARTMENTS),
    "business_unit": random.choice(["Manufacturing", "Corporate", "R&D", "Sales & Marketing"]),
    "region": random.choice(["AMER", "EMEA", "APAC", "LATAM"]),
} for i in range(100, 200)]

spark.createDataFrame(suppliers).write.mode("overwrite").saveAsTable("process_mining.reference.supplier_master")
spark.createDataFrame(contracts).write.mode("overwrite").saveAsTable("process_mining.reference.contracts")
spark.createDataFrame(cost_centers).write.mode("overwrite").saveAsTable("process_mining.reference.cost_centers")

print("Reference tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ```
# MAGIC Operational tables:
# MAGIC   purchase_requisitions  — 2,000 PRs (10% unapproved)
# MAGIC   purchase_orders        — ~2,700 POs (1-3 per PR, 5% unapproved)
# MAGIC   goods_receipts         — ~3,500 GRs (1-3 per PO, partial deliveries)
# MAGIC   invoices               — ~3,200 INVs (1-2 per PO, 15% uncleared)
# MAGIC   payments               — ~2,700 PAYs (only for cleared invoices)
# MAGIC
# MAGIC Reference tables:
# MAGIC   supplier_master        — 150 suppliers
# MAGIC   contracts              — 60 contracts
# MAGIC   cost_centers           — 100 cost centers
# MAGIC
# MAGIC Key relationships (why OCEL matters):
# MAGIC   PR → 1-3 POs (split by category)
# MAGIC   PO → 1-3 GRs (partial deliveries)
# MAGIC   PO → 1-2 Invoices (split billing)
# MAGIC   Invoice → 0-1 Payment (uncleared = no payment)
# MAGIC ```
# MAGIC
# MAGIC **Next:** Run the agentic skill:
# MAGIC
# MAGIC Claude Code: `/discover-event-log "Build event log from process_mining.erp_raw — P2P process"`
# MAGIC
# MAGIC Genie Code: `@discover-event-log Build event log from process_mining.erp_raw — P2P process`
# MAGIC
# MAGIC Ask for both traditional and OCEL output to compare.
