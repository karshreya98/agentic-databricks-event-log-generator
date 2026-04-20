"""
Process Mining Dashboard — Databricks App

Interactive dashboard that visualizes discovered process event logs.
Supports both traditional (single case_id) and OCEL (multi-object) formats.

Reads from Unity Catalog via SQL warehouse.
Visualization uses Plotly (no graphviz dependency).
"""

import os
import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import pm4py
from pm4py.statistics.traces.generic.pandas import case_statistics
from databricks.sdk import WorkspaceClient
from databricks import sql as db_sql

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATALOG = os.environ.get("CATALOG", "process_mining")
SCHEMA = os.environ.get("SCHEMA", "silver")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Process Mining Dashboard"
w = WorkspaceClient()


# ── Data Access ──

def _token() -> str:
    """Get a fresh bearer token from the app's service principal credentials."""
    headers = w.config.authenticate() or {}
    return headers.get("Authorization", "").replace("Bearer ", "")


def query(sql: str) -> pd.DataFrame:
    """Run SQL against the configured warehouse via databricks-sql-connector.

    Using the SQL connector (native Databricks protocol) instead of the Statement
    Execution API avoids two issues in Databricks Apps:
      1. 25MB inline-result cap (we'd otherwise need EXTERNAL_LINKS)
      2. Restricted app egress blocks presigned S3 URLs that EXTERNAL_LINKS returns
    The SQL connector streams results through the workspace endpoint, which is
    already allowlisted from the app's network.
    """
    hostname = w.config.host.replace("https://", "").replace("http://", "").rstrip("/")
    with db_sql.connect(
        server_hostname=hostname,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=_token(),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                return cur.fetchall_arrow().to_pandas()
            except Exception:
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                return pd.DataFrame(rows, columns=columns)


def discover_tables() -> list[dict]:
    """Find event log tables and OCEL tables in the catalog."""
    sql = f"""
        SELECT table_name FROM system.information_schema.tables
        WHERE table_catalog = '{CATALOG}' AND table_schema = '{SCHEMA}'
        ORDER BY table_name
    """
    df = query(sql)
    tables = []
    for name in df["table_name"].tolist():
        full = f"{CATALOG}.{SCHEMA}.{name}"
        if "ocel_e2o" in name or "ocel_objects" in name:
            continue  # skip OCEL support tables
        if "ocel_events" in name:
            tables.append({"label": f"{name} (OCEL)", "value": full, "type": "ocel"})
        else:
            # Check if it has case_id + activity + event_timestamp
            try:
                cols_df = query(f"DESCRIBE TABLE {full}")
                cols = cols_df["col_name"].tolist()
                if "case_id" in cols and "activity" in cols and "event_timestamp" in cols:
                    tables.append({"label": name, "value": full, "type": "traditional"})
            except Exception:
                pass
    return tables


def load_event_log(table: str) -> pd.DataFrame:
    # SELECT * so we adapt to whatever mandatory + optional columns the skill emitted.
    df = query(f"SELECT * FROM {table}")
    if df.empty:
        return df

    if "event_timestamp" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        sort_keys = [c for c in ("case_id", "event_timestamp") if c in df.columns]
        if sort_keys:
            df = df.sort_values(sort_keys).reset_index(drop=True)

    for col in ("cost", "time_since_prev_seconds", "event_rank"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if all(c in df.columns for c in ("case_id", "activity", "event_timestamp")):
        return pm4py.format_dataframe(
            df, case_id="case_id", activity_key="activity", timestamp_key="event_timestamp",
        )
    return df


def load_ocel_stats(events_table: str) -> dict:
    """Load OCEL-specific stats (objects, links)."""
    base = events_table.replace("_ocel_events", "")
    e2o_table = f"{base}_ocel_e2o"
    objects_table = f"{base}_ocel_objects"

    try:
        obj_df = query(f"SELECT object_type, COUNT(*) AS cnt FROM {objects_table} GROUP BY object_type ORDER BY cnt DESC")
        links_df = query(f"""
            SELECT links, COUNT(*) AS events FROM (
                SELECT event_id, COUNT(*) AS links FROM {e2o_table} GROUP BY event_id
            ) GROUP BY links ORDER BY links
        """)
        total_links = query(f"SELECT COUNT(*) AS cnt FROM {e2o_table}")
        return {
            "objects": obj_df,
            "links": links_df,
            "total_links": int(total_links["cnt"].iloc[0]),
        }
    except Exception:
        return None


# ── DFG Visualization ──

def build_dfg_figure(event_log: pd.DataFrame) -> go.Figure:
    dfg, start_acts, end_acts = pm4py.discover_dfg(event_log)
    act_counts = pm4py.get_event_attribute_values(event_log, "concept:name")

    if not dfg:
        return go.Figure().add_annotation(text="No process data", showarrow=False)

    activities = sorted(act_counts.keys())
    act_median_rank = (
        event_log.groupby("concept:name")["event_rank"].median().to_dict()
        if "event_rank" in event_log.columns
        else {a: i for i, a in enumerate(activities)}
    )

    rank_groups = {}
    for act in activities:
        r = round(act_median_rank.get(act, 0))
        rank_groups.setdefault(r, []).append(act)
    pos = {}
    for rank, group in rank_groups.items():
        for j, act in enumerate(group):
            pos[act] = (rank * 2, (j - (len(group) - 1) / 2) * 1.5)

    max_count = max(act_counts.values()) if act_counts else 1
    max_edge = max(dfg.values()) if dfg else 1
    fig = go.Figure()

    for (src, tgt), freq in dfg.items():
        if src not in pos or tgt not in pos:
            continue
        x0, y0 = pos[src]
        x1, y1 = pos[tgt]
        width = max(0.5, 4 * freq / max_edge)
        opacity = max(0.2, freq / max_edge)
        fig.add_trace(go.Scatter(
            x=[x0, x1, None], y=[y0, y1, None], mode="lines",
            line=dict(width=width, color=f"rgba(100,100,100,{opacity})"),
            hovertext=f"{src} → {tgt}<br>Freq: {freq:,}", hoverinfo="text", showlegend=False,
        ))
        mx, my = x0 * 0.3 + x1 * 0.7, y0 * 0.3 + y1 * 0.7
        fig.add_annotation(
            x=x1, y=y1, ax=mx, ay=my, xref="x", yref="y", axref="x", ayref="y",
            showarrow=True, arrowhead=2, arrowsize=1.5, arrowwidth=width,
            arrowcolor=f"rgba(100,100,100,{opacity})",
        )

    node_x = [pos[a][0] for a in activities if a in pos]
    node_y = [pos[a][1] for a in activities if a in pos]
    node_text = [a for a in activities if a in pos]
    node_size = [max(20, 60 * act_counts.get(a, 0) / max_count) for a in activities if a in pos]

    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        marker=dict(size=node_size, color="#1B3A5C", line=dict(width=2, color="white")),
        text=node_text, textposition="top center", textfont=dict(size=10),
        hovertext=[f"<b>{a}</b><br>Count: {act_counts.get(a,0):,}" for a in activities if a in pos],
        hoverinfo="text", showlegend=False,
    ))

    fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="white", margin=dict(l=20, r=20, t=20, b=20), height=450,
    )
    return fig


# ── Layout ──

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("Process Mining Dashboard"), width=6),
        dbc.Col(dcc.Dropdown(id="table-selector", placeholder="Select event log table..."), width=6),
    ], className="my-3"),

    # KPI row
    dbc.Row(id="kpi-cards", className="mb-3"),

    # Process map + variants
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

    # Bottlenecks + duration / OCEL stats
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Bottleneck Transitions"),
            dbc.CardBody(dcc.Graph(id="bottleneck-chart")),
        ]), width=6),
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="right-panel-header"),
            dbc.CardBody(id="right-panel-content"),
        ]), width=6),
    ], className="mb-3"),

    # Conformance
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Conformance: Fitness vs. Discovered Model"),
            dbc.CardBody(id="conformance-results"),
        ]), width=12),
    ]),
], fluid=True)


# ── Callbacks ──

@callback(
    Output("table-selector", "options"),
    Output("table-selector", "value"),
    Input("table-selector", "id"),
)
def populate_tables(_):
    tables = discover_tables()
    options = [{"label": t["label"], "value": t["value"]} for t in tables]
    default = options[0]["value"] if options else None
    return options, default


@callback(
    Output("kpi-cards", "children"),
    Output("process-map", "figure"),
    Output("variant-chart", "figure"),
    Output("bottleneck-chart", "figure"),
    Output("right-panel-header", "children"),
    Output("right-panel-content", "children"),
    Output("conformance-results", "children"),
    Input("table-selector", "value"),
)
def update_dashboard(table):
    empty = go.Figure().add_annotation(text="Select a table", showarrow=False)
    if not table:
        return [], empty, empty, empty, "Info", "Select an event log table above.", ""

    df = load_event_log(table)
    if df.empty:
        return [], empty, empty, empty, "Info", "No data in selected table.", ""

    # Detect if pm4py formatted (has case:concept:name) or raw columns
    is_pm4py = "case:concept:name" in df.columns
    case_col = "case:concept:name" if is_pm4py else ("case_id" if "case_id" in df.columns else "event_id")
    activity_col = "concept:name" if is_pm4py else "activity"

    # For OCEL events without case_id, use event_id as fallback for KPIs
    if case_col == "event_id" and "po_number" in df.columns:
        # Use po_number as a pseudo case_id for OCEL visualization
        df["case_id"] = df["po_number"]
        df = pm4py.format_dataframe(df, case_id="case_id", activity_key="activity", timestamp_key="event_timestamp")
        is_pm4py = True
        case_col = "case:concept:name"
        activity_col = "concept:name"

    # ── KPIs ──
    n_cases = df[case_col].nunique()
    n_events = len(df)
    n_activities = df[activity_col].nunique()

    try:
        case_descs = case_statistics.get_cases_description(df)
        durations_days = pd.Series([c["caseDuration"] / 86400 for c in case_descs.values()])
        median_dur = durations_days.median()
    except Exception:
        median_dur = 0

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
        acts = list(v) if isinstance(v, tuple) else v.split(",")
        label = " -> ".join(acts[:3]) + ("..." if len(acts) > 3 else "")
        v_labels.append(label)

    variant_fig = go.Figure(go.Bar(
        x=[v[1] for v in top_variants], y=v_labels,
        orientation="h", marker_color="#1B3A5C",
    ))
    variant_fig.update_layout(
        yaxis=dict(autorange="reversed"),
        margin=dict(l=10, r=10, t=10, b=10), height=400,
    )

    # ── Bottlenecks ──
    perf_dfg, _, _ = pm4py.discover_performance_dfg(df)

    def _perf_value(v):
        return v.get("mean", v.get("median", 0)) if isinstance(v, dict) else v

    edges = sorted(perf_dfg.items(), key=lambda x: -_perf_value(x[1]))[:10]
    bottleneck_fig = go.Figure(go.Bar(
        x=[_perf_value(e[1]) / 3600 for e in edges],
        y=[f"{e[0][0]} -> {e[0][1]}" for e in edges],
        orientation="h", marker_color="#E74C3C",
    ))
    bottleneck_fig.update_layout(
        yaxis=dict(autorange="reversed"), xaxis_title="Mean Hours",
        margin=dict(l=10, r=10, t=10, b=10), height=400,
    )

    # ── Right Panel: OCEL stats or Duration distribution ──
    ocel_stats = load_ocel_stats(table) if "ocel" in table else None

    if ocel_stats:
        # OCEL: show object types + links per event
        obj_df = ocel_stats["objects"]
        obj_fig = go.Figure(go.Bar(
            x=obj_df["cnt"].astype(int).tolist(),
            y=obj_df["object_type"].tolist(),
            orientation="h", marker_color="#2ECC71",
        ))
        obj_fig.update_layout(
            yaxis=dict(autorange="reversed"),
            margin=dict(l=10, r=10, t=10, b=10), height=150,
        )

        links_df = ocel_stats["links"]
        links_fig = go.Figure(go.Bar(
            x=links_df["links"].astype(int).tolist(),
            y=links_df["events"].astype(int).tolist(),
            marker_color="#3498DB",
        ))
        links_fig.update_layout(
            xaxis_title="Links per event", yaxis_title="Event count",
            margin=dict(l=10, r=10, t=10, b=10), height=150,
        )

        right_header = f"OCEL: {ocel_stats['total_links']:,} object links"
        right_content = html.Div([
            html.H6("Object Types"), dcc.Graph(figure=obj_fig, config={"displayModeBar": False}),
            html.H6("Links per Event"), dcc.Graph(figure=links_fig, config={"displayModeBar": False}),
        ])
    else:
        # Traditional: duration distribution
        duration_fig = px.histogram(durations_days, nbins=40,
            labels={"value": "Case Duration (days)", "count": "Cases"})
        duration_fig.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
        duration_fig.add_vline(x=median_dur, line_dash="dash", line_color="red",
            annotation_text=f"Median: {median_dur:.1f}d")

        right_header = "Case Duration Distribution"
        right_content = dcc.Graph(figure=duration_fig)

    # ── Conformance ──
    process_tree = pm4py.discover_process_tree_inductive(df)
    net, im, fm = pm4py.convert_to_petri_net(process_tree)
    fitness = pm4py.fitness_token_based_replay(df, net, im, fm)
    avg_fitness = fitness["average_trace_fitness"]
    pct_fitting = fitness.get("percentage_of_fitting_traces", avg_fitness * 100)

    conformance = dbc.Row([dbc.Col([
        dbc.Progress(
            value=avg_fitness * 100, label=f"Fitness: {avg_fitness:.1%}",
            color="success" if avg_fitness > 0.8 else "warning",
            className="mb-2", style={"height": "30px"},
        ),
        html.P(f"{pct_fitting:.0f}% of cases fully explained by the discovered model."),
        html.Small(
            "Measures how well the Inductive Miner's model fits observed behavior.",
            className="text-muted",
        ),
    ], width=12)])

    return kpis, process_map_fig, variant_fig, bottleneck_fig, right_header, right_content, conformance


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
