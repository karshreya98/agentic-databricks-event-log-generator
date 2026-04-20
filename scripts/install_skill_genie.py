# Databricks notebook source
# MAGIC %md
# MAGIC # Install `discover-event-log` skill for Genie Code
# MAGIC
# MAGIC Run this notebook once to install the skill into your workspace.
# MAGIC Equivalent to `scripts/install-genie-code.sh`, but runs entirely in the Databricks UI.
# MAGIC
# MAGIC After it completes:
# MAGIC 1. Open any notebook
# MAGIC 2. Open Genie Code (Assistant panel, top right) → switch to **Agent mode**
# MAGIC 3. Type: `@discover-event-log Build event logs from tables in <catalog>`

# COMMAND ----------

import base64
import urllib.request

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

SKILL_RAW_URL = (
    "https://raw.githubusercontent.com/karshreya98/"
    "agentic-databricks-event-log-generator/main/"
    "skills/discover-event-log/SKILL.md"
)

w = WorkspaceClient()
current_user = w.current_user.me().user_name

skill_dir = f"/Workspace/Users/{current_user}/.assistant/skills/discover-event-log"
skill_path = f"{skill_dir}/SKILL.md"

print(f"User:   {current_user}")
print(f"Target: {skill_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch `SKILL.md` from GitHub

# COMMAND ----------

with urllib.request.urlopen(SKILL_RAW_URL) as response:
    skill_content = response.read()

print(f"Fetched SKILL.md ({len(skill_content):,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install into workspace

# COMMAND ----------

w.workspace.mkdirs(skill_dir)

w.workspace.import_(
    path=skill_path,
    content=base64.b64encode(skill_content).decode("utf-8"),
    format=ImportFormat.AUTO,
    overwrite=True,
)

print(f"Installed at {skill_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC The cell below lists the installed skill file. You should see `SKILL.md` with a non-zero size.

# COMMAND ----------

for item in w.workspace.list(skill_dir):
    print(f"{item.object_type:10} {item.path}")
