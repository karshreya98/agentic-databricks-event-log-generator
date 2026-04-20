# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy + run the pm4py Databricks App
# MAGIC
# MAGIC Run this notebook once from inside the repo (cloned into a Databricks
# MAGIC Git folder, or imported preserving the `scripts/` + `app/` layout).
# MAGIC
# MAGIC What it does:
# MAGIC 1. Writes fevm-specific values into `app/app.yaml` (warehouse, catalog, schema)
# MAGIC 2. Creates the app (or reuses it if already there)
# MAGIC 3. Deploys the source code from `app/`
# MAGIC 4. Grants the app's service principal the UC + warehouse permissions it needs
# MAGIC 5. Prints the app URL
# MAGIC
# MAGIC Fill in the widgets, then **Run all**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure a recent `databricks-sdk` (Apps API support)

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet "databricks-sdk>=0.30.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("catalog", "process_mining", "Catalog")
dbutils.widgets.text("schema", "silver", "Schema")
dbutils.widgets.text("app_name", "process-mining-dashboard", "App name")

WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
APP_NAME = dbutils.widgets.get("app_name").strip()

assert WAREHOUSE_ID, "Set the 'SQL Warehouse ID' widget — find it under SQL Warehouses."
assert APP_NAME, "Set the 'App name' widget."

print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Catalog:   {CATALOG}")
print(f"Schema:    {SCHEMA}")
print(f"App:       {APP_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locate the `app/` folder relative to this notebook

# COMMAND ----------

from pathlib import Path

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
current_user = w.current_user.me().user_name

notebook_ws_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
notebook_fs = Path(f"/Workspace{notebook_ws_path}")
repo_root = notebook_fs.parent.parent  # scripts/<notebook> → repo root
app_fs = repo_root / "app"
app_ws = str(app_fs)  # e.g. /Workspace/Users/<user>/.../agentic-databricks-event-log-generator/app

if not (app_fs / "app.yaml").exists():
    raise FileNotFoundError(
        f"app.yaml not found under {app_fs}.\n"
        "Run this notebook from inside the repo (Databricks Git folder)."
    )

print(f"App source (filesystem): {app_fs}")
print(f"App source (workspace):  {app_ws}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update `app.yaml` with fevm values
# MAGIC
# MAGIC Writes the warehouse/catalog/schema widget values into the Git-folder copy of
# MAGIC `app.yaml`. This is a local edit — it won't push to GitHub unless you commit.

# COMMAND ----------

app_yaml = f"""command:
  - python
  - app.py
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "{WAREHOUSE_ID}"
  - name: CATALOG
    value: "{CATALOG}"
  - name: SCHEMA
    value: "{SCHEMA}"
"""

(app_fs / "app.yaml").write_text(app_yaml)
print("Wrote app.yaml:")
print(app_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create (or reuse) the app

# COMMAND ----------

from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import App

try:
    app = w.apps.get(name=APP_NAME)
    print(f"App '{APP_NAME}' already exists — reusing.")
except NotFound:
    print(f"Creating app '{APP_NAME}'...")
    app = w.apps.create_and_wait(app=App(name=APP_NAME))
    print(f"Created.")

print(f"App status: {app.compute_status.state if app.compute_status else 'unknown'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the source code

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

print(f"Deploying from {app_ws}...")
deployment = w.apps.deploy_and_wait(
    app_name=APP_NAME,
    app_deployment=AppDeployment(
        source_code_path=app_ws,
        mode=AppDeploymentMode.SNAPSHOT,
    ),
)
print(f"Deployment status: {deployment.status.state if deployment.status else 'unknown'}")

# Refresh app to get latest state + URL
app = w.apps.get(name=APP_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the app's service principal access
# MAGIC
# MAGIC Unity Catalog grants via SQL, warehouse CAN_USE via the permissions API.

# COMMAND ----------

sp_name = app.service_principal_client_id or app.service_principal_name
assert sp_name, "Could not determine app service principal."
print(f"App service principal: {sp_name}")

grants = [
    f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{sp_name}`",
    f"GRANT USE SCHEMA  ON SCHEMA  `{CATALOG}`.`{SCHEMA}` TO `{sp_name}`",
    f"GRANT SELECT      ON SCHEMA  `{CATALOG}`.`{SCHEMA}` TO `{sp_name}`",
]
for stmt in grants:
    print(f"▶ {stmt}")
    spark.sql(stmt)

# Warehouse CAN_USE — via permissions API
w.api_client.do(
    "PATCH",
    f"/api/2.0/permissions/warehouses/{WAREHOUSE_ID}",
    body={
        "access_control_list": [
            {
                "service_principal_name": sp_name,
                "permission_level": "CAN_USE",
            }
        ]
    },
)
print(f"▶ Granted CAN_USE on warehouse {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open the app

# COMMAND ----------

print(f"App name: {app.name}")
print(f"Status:   {app.compute_status.state if app.compute_status else 'unknown'}")
print(f"URL:      {app.url}")

displayHTML(f'<a href="{app.url}" target="_blank">Open {app.name} →</a>')
