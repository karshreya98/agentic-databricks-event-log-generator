"""
Process Mining Dashboard — Databricks App

Interactive process mining dashboard built with Dash + pm4py.
Reads refined event log data from Unity Catalog via SQL warehouse.

Visualization uses Plotly (no graphviz system dependency required).
"""

import os
import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import pm4py
from pm4py.statistics.traces.generic.pandas import case_statistics
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, StatementParameterListItem

# ── Config ──
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
EVENT_LOG_TABLE = os.environ.get("EVENT_LOG_TABLE", "process_mining.silver.event_log")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Process Mining Dashboard"

w = WorkspaceClient()


# ── Data Access ──

def query_lakehouse(sql: str, parameters: list[dict] | None = None) -> pd.DataFrame:
    """Execute a parameterized SQL query and return a DataFrame."""
    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql,
        parameters=parameters,
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise Exception(f"Query failed: {response.status.error}")
    columns = [col.name for col in response.manifest.schema.columns]
    rows = response.result.data_array if response.result and response.result.data_array else []
    return pd.DataFrame(rows, columns=columns)


# Detect which filter column exists in the table
FILTER_COL = os.environ.get("FILTER_COLUMN", "")


def _detect_filter_column() -> str:
    """Auto-detect the best filter column from the table schema."""
    global FILTER_COL
    if FILTER_COL:
        return FILTER_COL
    sql = f"DESCRIBE TABLE {EVENT_LOG_TABLE}"
    schema_df = query_lakehouse(sql)
    cols = schema_df["col_name"].tolist()
    # Try common filter columns in order of preference
    for candidate in ["department", "account_industry", "region", "source_system", "account_region"]:
        if candidate in cols:
            FILTER_COL = candidate
            return FILTER_COL
    FILTER_COL = ""
    return FILTER_COL


def load_event_log(filter_value: str | None = None) -> pd.DataFrame:
    """Load refined event log, optionally filtered."""
    filter_col = _detect_filter_column()

    if filter_value and filter_col:
        safe_val = filter_value.replace("'", "''")
        sql = f"""
            SELECT case_id, activity, event_timestamp,
                   resource, cost, time_since_prev_seconds, event_rank
            FROM {EVENT_LOG_TABLE}
            WHERE {filter_col} = '{safe_val}'
            ORDER BY case_id, event_timestamp
        """
    else:
        sql = f"""
            SELECT case_id, activity, event_timestamp,
                   resource, cost, time_since_prev_seconds, event_rank
            FROM {EVENT_LOG_TABLE}
            ORDER BY case_id, event_timestamp
        """

    df = query_lakehouse(sql)
    if df.empty:
        return df

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")
    df["time_since_prev_seconds"] = pd.to_numeric(df["time_since_prev_seconds"], errors="coerce")
    df["event_rank"] = pd.to_numeric(df["event_rank"], errors="coerce")

    return pm4py.format_dataframe(
        df, case_id="case_id", activity_key="activity", timestamp_key="event_timestamp",
    )


def load_filter_values() -> list[str]:
    """Load distinct values for the filter dropdown."""
    filter_col = _detect_filter_column()
    if not filter_col:
        return []
    sql = f"SELECT DISTINCT {filter_col} FROM {EVENT_LOG_TABLE} ORDER BY {filter_col}"
    df = query_lakehouse(sql)
    return df[filter_col].dropna().tolist()


# ── Plotly-based DFG Visualization (no graphviz needed) ──

def build_dfg_figure(event_log: pd.DataFrame) -> go.Figure:
    """
    Build a Directly-Follows Graph as a Plotly figure.
    Uses a layered layout based on median event rank per activity.
    """
    dfg, start_acts, end_acts = pm4py.discover_dfg(event_log)
    act_counts = pm4py.get_event_attribute_values(event_log, "concept:name")

    if not dfg:
        return go.Figure().add_annotation(text="No process data", showarrow=False)

    # Get all activities and compute their typical position in the process
    activities = sorted(act_counts.keys())
    act_median_rank = (
        event_log.groupby("concept:name")["event_rank"]
        .median()
        .to_dict()
        if "event_rank" in event_log.columns
        else {a: i for i, a in enumerate(activities)}
    )

    # Layout: x = median rank (left-to-right flow), y = spread vertically
    sorted_acts = sorted(activities, key=lambda a: act_median_rank.get(a, 0))
    pos = {}
    for i, act in enumerate(sorted_acts):
        pos[act] = (i * 2, 0)  # simple horizontal layout

    # If multiple activities share similar ranks, offset vertically
    rank_groups = {}
    for act in sorted_acts:
        r = round(act_median_rank.get(act, 0))
        rank_groups.setdefault(r, []).append(act)
    pos = {}
    for rank, group in rank_groups.items():
        for j, act in enumerate(group):
            y_offset = (j - (len(group) - 1) / 2) * 1.5
            pos[act] = (rank * 2, y_offset)

    max_count = max(act_counts.values()) if act_counts else 1
    max_edge = max(dfg.values()) if dfg else 1

    fig = go.Figure()

    # Draw edges
    for (src, tgt), freq in dfg.items():
        if src not in pos or tgt not in pos:
            continue
        x0, y0 = pos[src]
        x1, y1 = pos[tgt]
        width = max(0.5, 4 * freq / max_edge)
        opacity = max(0.2, freq / max_edge)

        fig.add_trace(go.Scatter(
            x=[x0, x1, None], y=[y0, y1, None],
            mode="lines",
            line=dict(width=width, color=f"rgba(100, 100, 100, {opacity})"),
            hoverinfo="text",
            text=f"{src} → {tgt}<br>Frequency: {freq:,}",
            showlegend=False,
        ))

        # Arrowhead (small triangle at midpoint-towards-target)
        mx = x0 * 0.3 + x1 * 0.7
        my = y0 * 0.3 + y1 * 0.7
        fig.add_annotation(
            x=x1, y=y1, ax=mx, ay=my,
            xref="x", yref="y", axref="x", ayref="y",
            showarrow=True, arrowhead=2, arrowsize=1.5,
            arrowwidth=width, arrowcolor=f"rgba(100,100,100,{opacity})",
        )

    # Draw nodes
    node_x = [pos[a][0] for a in activities if a in pos]
    node_y = [pos[a][1] for a in activities if a in pos]
    node_text = [a for a in activities if a in pos]
    node_size = [max(20, 60 * act_counts.get(a, 0) / max_count) for a in activities if a in pos]
    node_hover = [f"<b>{a}</b><br>Count: {act_counts.get(a, 0):,}" for a in activities if a in pos]

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        marker=dict(size=node_size, color="#1B3A5C", line=dict(width=2, color="white")),
        text=node_text,
        textposition="top center",
        textfont=dict(size=10),
        hovertext=node_hover,
        hoverinfo="text",
        showlegend=False,
    ))

    fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=20, b=20),
        height=450,
    )

    return fig


# ── Layout ──

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("Process Mining Dashboard"), width=8),
        dbc.Col(
            dcc.Dropdown(id="department-filter", placeholder="All (filter loading...)", clearable=True),
            width=4,
        ),
    ], className="my-3"),

    dbc.Row(id="kpi-cards", className="mb-3"),

    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Discovered Process Map (DFG)"),
            dbc.CardBody(dcc.Graph(id="process-map")),
        ]), width=8),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Top Process Variants"),
            dbc.CardBody(dcc.Graph(id="variant-chart")),
        ]), width=4),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Bottleneck Transitions (Median Hours)"),
            dbc.CardBody(dcc.Graph(id="bottleneck-chart")),
        ]), width=6),
        dbc.Col(dbc.Card([
            dbc.CardHeader("Case Duration Distribution"),
            dbc.CardBody(dcc.Graph(id="duration-chart")),
        ]), width=6),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Conformance: Fitness vs. Discovered Model"),
            dbc.CardBody(id="conformance-results"),
        ]), width=12),
    ]),
], fluid=True)


# ── Callbacks ──

@callback(
    Output("department-filter", "options"),
    Output("department-filter", "placeholder"),
    Input("department-filter", "id"),  # fires once on load
)
def populate_filter(_):
    values = load_filter_values()
    filter_col = FILTER_COL or "filter"
    label = filter_col.replace("_", " ").title()
    return [{"label": v, "value": v} for v in values], f"All {label}s"


@callback(
    Output("kpi-cards", "children"),
    Output("process-map", "figure"),
    Output("variant-chart", "figure"),
    Output("bottleneck-chart", "figure"),
    Output("duration-chart", "figure"),
    Output("conformance-results", "children"),
    Input("department-filter", "value"),
)
def update_dashboard(department):
    df = load_event_log(department)

    if df.empty:
        empty = go.Figure().add_annotation(text="No data", showarrow=False)
        return [], empty, empty, empty, empty, "No data available."

    # ── KPIs ──
    n_cases = df["case:concept:name"].nunique()
    n_events = len(df)
    n_activities = df["concept:name"].nunique()

    case_descs = case_statistics.get_cases_description(df)
    durations_days = pd.Series([c["caseDuration"] / 86400 for c in case_descs.values()])
    median_dur = durations_days.median()

    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{n_cases:,}"), html.P("Cases"),
        ]), color="primary", inverse=True), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{n_events:,}"), html.P("Events"),
        ]), color="info", inverse=True), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{n_activities}"), html.P("Activities"),
        ]), color="success", inverse=True), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4(f"{median_dur:.1f}d"), html.P("Median Duration"),
        ]), color="warning", inverse=True), width=3),
    ])

    # ── Process Map ──
    process_map_fig = build_dfg_figure(df)

    # ── Variants ──
    variants = pm4py.get_variants(df)
    top_variants = sorted(variants.items(), key=lambda x: -x[1])[:10]
    v_labels = []
    for v, _ in top_variants:
        # pm4py returns variant keys as tuples of activity names
        acts = list(v) if isinstance(v, tuple) else v.split(",")
        label = " → ".join(acts[:3]) + ("..." if len(acts) > 3 else "")
        v_labels.append(label)

    variant_fig = go.Figure(go.Bar(
        x=[v[1] for v in top_variants],
        y=v_labels,
        orientation="h",
        marker_color="#1B3A5C",
    ))
    variant_fig.update_layout(
        yaxis=dict(autorange="reversed"),
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
    )

    # ── Bottlenecks ──
    perf_dfg, _, _ = pm4py.discover_performance_dfg(df)
    # pm4py returns dicts with 'mean','median','min','max' — extract mean seconds
    def _perf_value(v):
        if isinstance(v, dict):
            return v.get("mean", v.get("median", 0))
        return v

    edges = sorted(perf_dfg.items(), key=lambda x: -_perf_value(x[1]))[:10]
    bottleneck_fig = go.Figure(go.Bar(
        x=[_perf_value(e[1]) / 3600 for e in edges],
        y=[f"{e[0][0]} → {e[0][1]}" for e in edges],
        orientation="h",
        marker_color="#E74C3C",
    ))
    bottleneck_fig.update_layout(
        yaxis=dict(autorange="reversed"),
        xaxis_title="Mean Hours",
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
    )

    # ── Duration Distribution ──
    duration_fig = px.histogram(
        durations_days, nbins=40,
        labels={"value": "Case Duration (days)", "count": "Cases"},
    )
    duration_fig.update_layout(
        showlegend=False,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    duration_fig.add_vline(
        x=median_dur, line_dash="dash", line_color="red",
        annotation_text=f"Median: {median_dur:.1f}d",
    )

    # ── Conformance ──
    process_tree = pm4py.discover_process_tree_inductive(df)
    net, im, fm = pm4py.convert_to_petri_net(process_tree)
    fitness = pm4py.fitness_token_based_replay(df, net, im, fm)
    avg_fitness = fitness["average_trace_fitness"]
    pct_fitting = fitness.get("percentage_of_fitting_traces", avg_fitness * 100)

    conformance = dbc.Row([
        dbc.Col([
            dbc.Progress(
                value=avg_fitness * 100,
                label=f"Fitness: {avg_fitness:.1%}",
                color="success" if avg_fitness > 0.8 else "warning",
                className="mb-2",
                style={"height": "30px"},
            ),
            html.P(
                f"{pct_fitting:.0f}% of cases are fully explained by the discovered model. "
                f"Remaining cases contain deviations (rework loops, skipped steps, exceptions)."
            ),
            html.Small(
                "Note: this measures how well the Inductive Miner's model fits the observed data. "
                "For compliance checking against a prescribed process, see the analysis notebook.",
                className="text-muted",
            ),
        ], width=12),
    ])

    return kpis, process_map_fig, variant_fig, bottleneck_fig, duration_fig, conformance


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
