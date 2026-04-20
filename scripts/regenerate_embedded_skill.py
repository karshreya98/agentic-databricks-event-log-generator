#!/usr/bin/env python3
"""Regenerate scripts/install_skill_genie.py with the current SKILL.md embedded.

Run this whenever skills/discover-event-log/SKILL.md changes so the notebook
installer stays in sync. The notebook embeds SKILL.md as base64 so it works
even when the source repo is private (no network fetch needed).

Usage:
    python3 scripts/regenerate_embedded_skill.py
"""
import base64
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_PATH = REPO_ROOT / "skills" / "discover-event-log" / "SKILL.md"
NOTEBOOK_PATH = REPO_ROOT / "scripts" / "install_skill_genie.py"

raw = SKILL_PATH.read_bytes()
b64 = base64.b64encode(raw).decode("utf-8")
lines = textwrap.wrap(b64, 76)
embedded = "\n".join(f'    "{line}"' for line in lines)

notebook = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Install `discover-event-log` skill for Genie Code
# MAGIC
# MAGIC Run this notebook once to install the skill into your workspace.
# MAGIC Equivalent to `scripts/install-genie-code.sh`, but runs entirely in the Databricks UI.
# MAGIC
# MAGIC The `SKILL.md` content is embedded below (base64), so no network fetch is needed —
# MAGIC the notebook works even if the source repo is private.
# MAGIC
# MAGIC After it completes:
# MAGIC 1. Open any notebook
# MAGIC 2. Open Genie Code (Assistant panel, top right) → switch to **Agent mode**
# MAGIC 3. Type: `@discover-event-log Build event logs from tables in <catalog>`

# COMMAND ----------

import base64

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()
current_user = w.current_user.me().user_name

skill_dir = f"/Workspace/Users/{{current_user}}/.assistant/skills/discover-event-log"
skill_path = f"{{skill_dir}}/SKILL.md"

print(f"User:   {{current_user}}")
print(f"Target: {{skill_path}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedded `SKILL.md` content
# MAGIC
# MAGIC Regenerate with `scripts/regenerate_embedded_skill.py` whenever `skills/discover-event-log/SKILL.md` changes.

# COMMAND ----------

SKILL_MD_B64 = (
{embedded}
)

skill_content = base64.b64decode(SKILL_MD_B64)
print(f"Decoded SKILL.md ({{len(skill_content):,}} bytes)")

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

print(f"Installed at {{skill_path}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

for item in w.workspace.list(skill_dir):
    print(f"{{item.object_type:10}} {{item.path}}")
'''

NOTEBOOK_PATH.write_text(notebook)
print(f"Regenerated {NOTEBOOK_PATH.relative_to(REPO_ROOT)}")
print(f"  SKILL.md: {len(raw):,} bytes → base64: {len(b64):,} chars")
