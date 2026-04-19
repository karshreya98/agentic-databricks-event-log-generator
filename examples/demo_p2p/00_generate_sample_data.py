# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Procure-to-Pay Event Log
# MAGIC
# MAGIC Creates a realistic P2P event log with:
# MAGIC - Inconsistent activity names (to demonstrate refinement)
# MAGIC - Varying case durations and paths
# MAGIC - Rework loops, skipped steps, and SLA breaches
# MAGIC - Multiple departments and resources

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS process_mining;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS process_mining.silver;

# COMMAND ----------

# P2P process definition
# The "happy path" is: PR → PO → GR → IR → Pay
# We simulate real-world messiness: loops, skips, variant names

ACTIVITIES_HAPPY_PATH = [
    "Create Purchase Requisition",
    "Approve Purchase Requisition",
    "Create Purchase Order",
    "Approve Purchase Order",
    "Post Goods Receipt",
    "Receive Invoice",
    "Clear Invoice",
    "Process Payment",
]

# Messy activity name variants (what raw source systems actually produce)
ACTIVITY_VARIANTS = {
    "Create Purchase Requisition": [
        "Create Purchase Requisition",
        "PR Created",
        "purchase_requisition_created",
        "Create PR",
    ],
    "Approve Purchase Requisition": [
        "Approve Purchase Requisition",
        "PR Approved",
        "Approve PR",
        "purchase_req_approved",
    ],
    "Create Purchase Order": [
        "Create Purchase Order",
        "PO Created",
        "CREATE_PO",
        "Create PO",
    ],
    "Approve Purchase Order": [
        "Approve Purchase Order",
        "PO Approved",
        "Approve PO",
        "purchase_order_approved",
    ],
    "Post Goods Receipt": [
        "Post Goods Receipt",
        "GR Posted",
        "Goods Receipt",
        "goods_receipt_posted",
    ],
    "Receive Invoice": [
        "Receive Invoice",
        "Invoice Received",
        "Inv Received",
        "INVOICE_RECEIVED",
    ],
    "Clear Invoice": [
        "Clear Invoice",
        "Invoice Cleared",
        "Clear Inv",
        "invoice_cleared",
    ],
    "Process Payment": [
        "Process Payment",
        "Payment Sent",
        "Paid",
        "payment_processed",
    ],
}

# Rework / exception activities
REWORK_ACTIVITIES = [
    "Return to Supplier",
    "Rework Purchase Requisition",
    "Reject Invoice",
    "Request Price Correction",
    "Escalate to Manager",
]

DEPARTMENTS = ["Procurement", "Finance", "Warehouse", "Accounts Payable", "Management"]
RESOURCES = [
    "alice.jones", "bob.smith", "carol.wu", "dave.patel",
    "emma.garcia", "frank.kim", "grace.lee", "henry.chen",
    "iris.taylor", "jack.wilson",
]
SOURCE_SYSTEMS = ["SAP_ECC", "SAP_S4", "Ariba", "Coupa"]

# COMMAND ----------

def generate_case(case_id: str, start_time: datetime) -> list:
    """Generate a single P2P case with realistic variation."""
    events = []
    current_time = start_time
    source_system = random.choice(SOURCE_SYSTEMS)
    department_map = {
        "Create Purchase Requisition": "Procurement",
        "Approve Purchase Requisition": "Management",
        "Create Purchase Order": "Procurement",
        "Approve Purchase Order": "Management",
        "Post Goods Receipt": "Warehouse",
        "Receive Invoice": "Accounts Payable",
        "Clear Invoice": "Finance",
        "Process Payment": "Finance",
    }

    # Decide the case profile
    profile = random.choices(
        ["happy", "rework", "partial", "fast"],
        weights=[0.50, 0.25, 0.10, 0.15],
        k=1,
    )[0]

    if profile == "happy":
        activities = list(ACTIVITIES_HAPPY_PATH)
    elif profile == "rework":
        # Insert 1-2 rework loops
        activities = list(ACTIVITIES_HAPPY_PATH)
        rework_point = random.randint(2, 5)
        rework_activity = random.choice(REWORK_ACTIVITIES)
        activities.insert(rework_point, rework_activity)
        # Sometimes re-do the prior step after rework
        if random.random() < 0.6:
            activities.insert(rework_point + 1, ACTIVITIES_HAPPY_PATH[rework_point - 1])
    elif profile == "partial":
        # Case that doesn't complete (drops off partway)
        cutoff = random.randint(3, 6)
        activities = ACTIVITIES_HAPPY_PATH[:cutoff]
    else:  # fast
        # Skip some approvals (auto-approved low-value POs)
        activities = [a for a in ACTIVITIES_HAPPY_PATH if "Approve" not in a]

    for activity in activities:
        # Use a messy variant name from source system
        if activity in ACTIVITY_VARIANTS:
            raw_activity = random.choice(ACTIVITY_VARIANTS[activity])
        else:
            raw_activity = activity

        dept = department_map.get(activity, random.choice(DEPARTMENTS))
        resource = random.choice(RESOURCES)

        # Time between activities: 1 hour to 5 days, with some steps faster
        if "Approve" in activity:
            delay_hours = random.uniform(4, 120)  # approvals can be slow
        elif "Payment" in activity:
            delay_hours = random.uniform(24, 240)  # payments are slowest
        else:
            delay_hours = random.uniform(1, 48)

        current_time += timedelta(hours=delay_hours)
        cost = round(random.uniform(5.0, 500.0), 2) if "Payment" in activity else round(random.uniform(0.5, 25.0), 2)

        events.append({
            "event_id": f"{case_id}-{len(events)+1:03d}",
            "case_id": case_id,
            "activity": raw_activity,
            "timestamp": current_time,
            "resource": resource,
            "cost": cost,
            "department": dept,
            "source_system": source_system,
            "raw_payload": f'{{"original_activity": "{raw_activity}", "system": "{source_system}"}}',
        })

    return events

# COMMAND ----------

# Generate 5,000 cases spanning 6 months
random.seed(42)
all_events = []

for i in range(5000):
    case_id = f"P2P-2024-{i+1:05d}"
    # Cases start randomly over a 6-month window
    start_offset = random.uniform(0, 180 * 24)  # hours
    start_time = datetime(2024, 1, 1) + timedelta(hours=start_offset)
    all_events.extend(generate_case(case_id, start_time))

print(f"Generated {len(all_events)} events across 5,000 cases")

# COMMAND ----------

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("case_id", StringType(), False),
    StructField("activity", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("resource", StringType(), True),
    StructField("cost", DoubleType(), True),
    StructField("department", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

df = spark.createDataFrame(all_events, schema=schema)

(
    df.write
    .mode("overwrite")
    .saveAsTable("process_mining.bronze.event_logs")
)

print(f"Wrote {df.count()} events to process_mining.bronze.event_logs")

# COMMAND ----------

# Quick sanity check
display(spark.sql("""
    SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT case_id) as total_cases,
        COUNT(DISTINCT activity) as raw_activity_names,
        MIN(timestamp) as earliest,
        MAX(timestamp) as latest
    FROM process_mining.bronze.event_logs
"""))
