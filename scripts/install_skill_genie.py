# Databricks notebook source
# MAGIC %md
# MAGIC # Install `discover-event-log` skill for Genie Code
# MAGIC
# MAGIC Run this notebook once from inside the repo (cloned into a Databricks
# MAGIC Git folder, or imported preserving the `scripts/` + `skills/` layout).
# MAGIC Equivalent to `scripts/install-genie-code.sh`, but runs entirely in the UI.
# MAGIC
# MAGIC After it completes:
# MAGIC 1. Open any notebook
# MAGIC 2. Open Genie Code (Assistant panel, top right) → switch to **Agent mode**
# MAGIC 3. Type: `@discover-event-log Build event logs from tables in <catalog>`

# COMMAND ----------

import shutil
from pathlib import Path

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
current_user = w.current_user.me().user_name

# Locate SKILL.md relative to this notebook: ../skills/discover-event-log/SKILL.md
notebook_ws_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
notebook_fs = Path(f"/Workspace{notebook_ws_path}")
repo_root = notebook_fs.parent.parent  # scripts/<notebook> → repo root
skill_source = repo_root / "skills" / "discover-event-log" / "SKILL.md"

if not skill_source.exists():
    raise FileNotFoundError(
        f"SKILL.md not found at {skill_source}.\n"
        "Make sure this notebook is run from inside the repo "
        "(e.g. cloned via Databricks Git folders)."
    )

print(f"User:   {current_user}")
print(f"Source: {skill_source}")
print(f"Size:   {skill_source.stat().st_size:,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy into `.assistant/skills/`

# COMMAND ----------

skill_dest_dir = f"/Workspace/Users/{current_user}/.assistant/skills/discover-event-log"
skill_dest = f"{skill_dest_dir}/SKILL.md"

w.workspace.mkdirs(skill_dest_dir)
shutil.copyfile(skill_source, skill_dest)

print(f"Installed at {skill_dest}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

for item in w.workspace.list(skill_dest_dir):
    print(f"{item.object_type:10} {item.path}")
