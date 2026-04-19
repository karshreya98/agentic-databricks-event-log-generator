# Databricks notebook source
# MAGIC %md
# MAGIC # Process Mining Analysis with pm4py
# MAGIC
# MAGIC This notebook runs process discovery, conformance checking, and performance
# MAGIC analysis on the refined silver event log.
# MAGIC
# MAGIC **Important:** pm4py operates on pandas DataFrames (single-node). Spark handles
# MAGIC the heavy lifting of refinement at scale; pm4py handles the algorithmic analysis
# MAGIC on the refined result. For very large event logs (10M+ events), filter to a
# MAGIC specific process, time window, or sample before calling `.toPandas()`.

# COMMAND ----------

# MAGIC %pip install pm4py
# dbutils.library.restartPython()

# COMMAND ----------

import pm4py
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Silver Event Log
# MAGIC
# MAGIC The silver table may contain millions of events refined by Spark.
# MAGIC We filter or sample before pulling into pandas for pm4py analysis.

# COMMAND ----------

# For large datasets, filter by time window or sample
silver_df = (
    spark.table("process_mining.silver.event_log")
    # Example: filter to a 3-month window
    # .filter("event_timestamp >= '2024-01-01' AND event_timestamp < '2024-04-01'")
    .toPandas()
)

print(f"Loaded {len(silver_df):,} events, {silver_df['case_id'].nunique():,} cases")

# COMMAND ----------

# Format for pm4py
event_log = pm4py.format_dataframe(
    silver_df,
    case_id="case_id",
    activity_key="activity",
    timestamp_key="event_timestamp",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Discovery
# MAGIC
# MAGIC Discover the actual process model from the event log — no prior model needed.
# MAGIC The Inductive Miner produces a sound process tree that balances fitness and
# MAGIC precision.

# COMMAND ----------

# Directly-Follows Graph (DFG) — shows activity transitions and frequencies
dfg, start_activities, end_activities = pm4py.discover_dfg(event_log)

pm4py.view_dfg(dfg, start_activities, end_activities, format="png")

# COMMAND ----------

# Inductive Miner — discovers a structured process tree
process_tree = pm4py.discover_process_tree_inductive(event_log)
pm4py.view_process_tree(process_tree, format="png")

# COMMAND ----------

# Convert to BPMN for a business-friendly view
bpmn_model = pm4py.convert_to_bpmn(process_tree)
pm4py.view_bpmn(bpmn_model, format="png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variant Analysis
# MAGIC
# MAGIC A "variant" is a unique path through the process. The happy path should
# MAGIC dominate, but rework loops and exceptions create long-tail variants.
# MAGIC This is where you find the process reality vs. the process design.

# COMMAND ----------

variants = pm4py.get_variants(event_log)

print(f"Total unique variants: {len(variants)}")
print(f"\nTop 10 variants:")
for variant, count in sorted(variants.items(), key=lambda x: -x[1])[:10]:
    activities = variant.split(",")
    label = " → ".join(activities)
    print(f"  [{count:>5} cases] {label}")

# COMMAND ----------

# Variant distribution — how concentrated is the process?
sorted_counts = sorted(variants.values(), reverse=True)
cumulative = pd.Series(sorted_counts).cumsum() / sum(sorted_counts)

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(range(1, len(cumulative) + 1), cumulative.values, marker="o", markersize=3)
ax.set_xlabel("Number of Variants")
ax.set_ylabel("Cumulative % of Cases")
ax.set_title("Process Variant Concentration")
ax.axhline(y=0.8, color="red", linestyle="--", alpha=0.5, label="80% of cases")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conformance Checking
# MAGIC
# MAGIC Conformance checking compares observed behavior against a reference model.
# MAGIC
# MAGIC Two approaches:
# MAGIC 1. **Self-check** — discover a model, then measure fitness against the same data.
# MAGIC    Tells you how well the algorithm captures the process (useful for model selection).
# MAGIC 2. **Reference check** — compare against a *prescribed* model (the intended process).
# MAGIC    Tells you how much the real process deviates from the design.
# MAGIC
# MAGIC We show both below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 1: Self-Check (model quality)
# MAGIC How well does the Inductive Miner's model explain the observed behavior?

# COMMAND ----------

fitness = pm4py.fitness_token_based_replay(event_log, process_tree)
print(f"Average trace fitness: {fitness['average_trace_fitness']:.2%}")
print(f"  → Percentage of cases fully explained: {fitness['percentage_of_fitting_traces']:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 2: Reference Check (process compliance)
# MAGIC Define the intended "happy path" as a reference model, then measure
# MAGIC how many cases deviate from it.

# COMMAND ----------

# Build a reference Petri net for the intended happy path
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.utils import petri_utils

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

# Build a sequential Petri net for the happy path
net = PetriNet("p2p_reference")
places = [PetriNet.Place(f"p{i}") for i in range(len(HAPPY_PATH) + 1)]
for p in places:
    net.places.add(p)

for i, activity in enumerate(HAPPY_PATH):
    t = PetriNet.Transition(f"t{i}", activity)
    net.transitions.add(t)
    petri_utils.add_arc_from_to(places[i], t, net)
    petri_utils.add_arc_from_to(t, places[i + 1], net)

initial_marking = Marking({places[0]: 1})
final_marking = Marking({places[-1]: 1})

# Check conformance against the reference
ref_fitness = pm4py.fitness_token_based_replay(
    event_log, net, initial_marking, final_marking
)
print(f"Fitness vs. reference model: {ref_fitness['average_trace_fitness']:.2%}")
print(f"Cases following happy path exactly: {ref_fitness['percentage_of_fitting_traces']:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Analysis
# MAGIC
# MAGIC Identify bottlenecks — which transitions take the longest?

# COMMAND ----------

# Performance DFG: edges weighted by median transition time
perf_dfg, perf_sa, perf_ea = pm4py.discover_performance_dfg(event_log)

# Top 10 slowest transitions
print("Top 10 slowest transitions (median hours):")
for edge, seconds in sorted(perf_dfg.items(), key=lambda x: -x[1])[:10]:
    hours = seconds / 3600
    print(f"  {edge[0]:40s} → {edge[1]:30s}  {hours:8.1f}h")

# COMMAND ----------

pm4py.view_performance_dfg(perf_dfg, perf_sa, perf_ea, format="png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case Duration Analysis

# COMMAND ----------

from pm4py.statistics.traces.generic.pandas import case_statistics

case_stats = case_statistics.get_cases_description(event_log)
durations_days = pd.Series(
    {cid: info["caseDuration"] / 86400 for cid, info in case_stats.items()}
)

fig, axes = plt.subplots(1, 2, figsize=(14, 4))

axes[0].hist(durations_days, bins=50, color="#1B3A5C", edgecolor="white")
axes[0].set_xlabel("Case Duration (days)")
axes[0].set_ylabel("Number of Cases")
axes[0].set_title("Distribution of Case Durations")
axes[0].axvline(durations_days.median(), color="red", linestyle="--", label=f"Median: {durations_days.median():.1f}d")
axes[0].legend()

# Duration by variant (top 5)
top_5_variants = sorted(variants.items(), key=lambda x: -x[1])[:5]
variant_names = [v[0] for v in top_5_variants]
variant_durations = []
for vname in variant_names:
    cases_in_variant = pm4py.filter_variants(event_log, [vname])
    v_stats = case_statistics.get_cases_description(cases_in_variant)
    v_dur = [info["caseDuration"] / 86400 for info in v_stats.values()]
    variant_durations.append(v_dur)

axes[1].boxplot(variant_durations, vert=True)
axes[1].set_xticklabels([f"V{i+1}" for i in range(5)], fontsize=9)
axes[1].set_ylabel("Duration (days)")
axes[1].set_title("Duration by Top 5 Variants")

plt.tight_layout()
plt.show()
