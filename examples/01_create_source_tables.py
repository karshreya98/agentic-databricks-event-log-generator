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

# MAGIC %md
# MAGIC ## Configure target catalog
# MAGIC
# MAGIC Set the catalog to write into. Schemas (`erp_raw`, `reference`, `silver`) are
# MAGIC created under it. You need `CREATE SCHEMA` on the catalog — and `CREATE CATALOG`
# MAGIC on the metastore if the catalog doesn't exist yet.

# COMMAND ----------

dbutils.widgets.text("catalog", "process_mining", "Target catalog")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Set the 'catalog' widget."
print(f"Target catalog: {CATALOG}")

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception:
    pass

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.erp_raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.reference")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
print(f"Schemas ready: {CATALOG}.{{erp_raw, reference, silver}}")

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

print(f"Generated (before noise):")
print(f"  PRs:       {len(prs):,}")
print(f"  POs:       {len(pos):,}")
print(f"  GRs:       {len(grs):,}")
print(f"  Invoices:  {len(invoices):,}")
print(f"  Payments:  {len(payments):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Realistic Noise
# MAGIC
# MAGIC Real data is messy. We inject two kinds of noise:
# MAGIC
# MAGIC **Data-quality noise** (affects enrichment / joins, not process flow):
# MAGIC - **Duplicate events** (3% of GRs posted twice)
# MAGIC - **Null timestamps** (2% of POs missing approved_at even though status = approved)
# MAGIC - **Orphan records** (invoices referencing non-existent POs)
# MAGIC - **Inconsistent supplier IDs** (typos, case differences)
# MAGIC - **Missing enrichment keys** (5% of POs have null supplier_id)
# MAGIC
# MAGIC **Process-variant noise** (creates traces that deviate from the happy path —
# MAGIC this is what drives fitness < 100% during conformance checking):
# MAGIC - **Out-of-order GRs** (6% posted before PO approval)
# MAGIC - **Invoice-before-GR** (5% of POs are billed before any goods receipt — maverick buying)
# MAGIC - **Early payment** (3% of payments happen before invoice cleared_date — prepayments)
# MAGIC - **Rogue POs** (3% of POs have no linking PR — off-contract buying)
# MAGIC - **PO amendments** (8% of POs are modified after approval — rework loop)

# COMMAND ----------

# Duplicate ~3% of goods receipts (same GR posted twice)
num_dupes = int(len(grs) * 0.03)
dupe_indices = random.sample(range(len(grs)), num_dupes)
for idx in dupe_indices:
    dupe = dict(grs[idx])
    dupe["gr_id"] = dupe["gr_id"] + "-DUP"
    grs.append(dupe)

# Null timestamps: 2% of approved POs have approved_at = None (data quality issue)
num_null_ts = int(len(pos) * 0.02)
for idx in random.sample(range(len(pos)), num_null_ts):
    if pos[idx]["status"] == "approved":
        pos[idx]["approved_at"] = None

# Orphan invoices: 20 invoices referencing POs that don't exist
for i in range(20):
    invoices.append({
        "invoice_id": f"INV-ORPHAN-{i+1:03d}",
        "po_id": f"PO-GHOST-{i+1:05d}-1",
        "supplier_id": f"SUP-{random.randint(0, 149):04d}",
        "received_date": datetime(2024, 3, 1) + timedelta(hours=random.uniform(0, 720)),
        "cleared_date": None,
        "invoice_amount": round(random.uniform(100, 5000), 2),
        "three_way_match": "fail",
    })

# Inconsistent supplier IDs: 1% of POs have typos (extra space, lowercase)
num_typo = int(len(pos) * 0.01)
for idx in random.sample(range(len(pos)), num_typo):
    pos[idx]["supplier_id"] = pos[idx]["supplier_id"].lower() + " "

# Out-of-order: 6% of GRs have posting_date BEFORE the PO approval
num_ooo = int(len(grs) * 0.06)
for idx in random.sample(range(len(grs)), min(num_ooo, len(grs))):
    grs[idx]["posting_date"] = grs[idx]["posting_date"] - timedelta(days=random.randint(5, 30))

# Missing enrichment keys: 5% of POs have null supplier_id
num_null_sup = int(len(pos) * 0.05)
for idx in random.sample(range(len(pos)), num_null_sup):
    pos[idx]["supplier_id"] = None

# Invoice-before-GR: 5% of POs get billed before any goods receipt is posted.
# Shift every invoice for the PO to before the earliest GR date for that PO.
po_to_invoices = {}
for inv in invoices:
    po_to_invoices.setdefault(inv["po_id"], []).append(inv)
po_to_grs = {}
for gr in grs:
    po_to_grs.setdefault(gr["po_id"], []).append(gr)

po_ids_with_both = [pid for pid in po_to_invoices if pid in po_to_grs]
num_early_inv = int(len(po_ids_with_both) * 0.05)
early_inv_po_ids = random.sample(po_ids_with_both, min(num_early_inv, len(po_ids_with_both)))
for pid in early_inv_po_ids:
    earliest_gr = min(gr["posting_date"] for gr in po_to_grs[pid])
    for inv in po_to_invoices[pid]:
        inv["received_date"] = earliest_gr - timedelta(hours=random.uniform(24, 168))
        # If the invoice was cleared, slide clear_date earlier too to keep it after receipt
        if inv["cleared_date"] is not None:
            inv["cleared_date"] = inv["received_date"] + timedelta(hours=random.uniform(24, 168))

# Early payment: 3% of payments happen BEFORE the invoice cleared_date (prepayments)
inv_by_id = {inv["invoice_id"]: inv for inv in invoices}
num_early_pay = int(len(payments) * 0.03)
for pay in random.sample(payments, min(num_early_pay, len(payments))):
    inv = inv_by_id.get(pay["invoice_id"])
    if inv and inv.get("cleared_date"):
        pay["payment_date"] = inv["cleared_date"] - timedelta(hours=random.uniform(12, 96))

# Rogue POs: 3% of POs have no linking PR (off-contract / emergency buying).
# These cases will be missing the PR_Created / PR_Approved events upstream.
num_rogue = int(len(pos) * 0.03)
for idx in random.sample(range(len(pos)), num_rogue):
    pos[idx]["pr_id"] = None

# PO amendments: 8% of approved POs get a modified_at timestamp AFTER approval
# (rework loop — discovery will surface this as a PO_Modified activity).
for po in pos:
    po["modified_at"] = None
num_amended = int(len(pos) * 0.08)
amend_candidates = [i for i, po in enumerate(pos) if po.get("approved_at") is not None]
for idx in random.sample(amend_candidates, min(num_amended, len(amend_candidates))):
    pos[idx]["modified_at"] = pos[idx]["approved_at"] + timedelta(hours=random.uniform(12, 240))

print(f"\nNoise added:")
print(f"  Data quality:")
print(f"    Duplicate GRs:          {num_dupes}")
print(f"    Null PO approved_at:    {num_null_ts}")
print(f"    Orphan invoices:        20")
print(f"    Supplier ID typos:      {num_typo}")
print(f"    Null supplier_ids:      {num_null_sup}")
print(f"  Process variants (drive fitness < 100%):")
print(f"    Out-of-order GRs:       {num_ooo}")
print(f"    Invoice-before-GR POs:  {len(early_inv_po_ids)}")
print(f"    Early payments:         {num_early_pay}")
print(f"    Rogue POs (no PR):      {num_rogue}")
print(f"    PO amendments:          {num_amended}")

print(f"\nFinal counts:")
print(f"  PRs:       {len(prs):,}")
print(f"  POs:       {len(pos):,}")
print(f"  GRs:       {len(grs):,}")
print(f"  Invoices:  {len(invoices):,}")
print(f"  Payments:  {len(payments):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Operational Tables

# COMMAND ----------

def _write(df_rows, table):
    spark.createDataFrame(df_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

_write(prs,      f"{CATALOG}.erp_raw.purchase_requisitions")
_write(pos,      f"{CATALOG}.erp_raw.purchase_orders")
_write(grs,      f"{CATALOG}.erp_raw.goods_receipts")
_write(invoices, f"{CATALOG}.erp_raw.invoices")
_write(payments, f"{CATALOG}.erp_raw.payments")

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

_write(suppliers,    f"{CATALOG}.reference.supplier_master")
_write(contracts,    f"{CATALOG}.reference.contracts")
_write(cost_centers, f"{CATALOG}.reference.cost_centers")

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
# MAGIC **Next:** Run the agentic skill against the catalog you just populated.
# MAGIC
# MAGIC Claude Code: `/discover-event-log "Build event logs from tables in <CATALOG>"`
# MAGIC
# MAGIC Genie Code: `@discover-event-log Build event logs from tables in <CATALOG>`
# MAGIC
# MAGIC Replace `<CATALOG>` with whatever you set the `catalog` widget to.
# MAGIC The skill will ask whether to also produce OCEL output if it detects multi-object data.
