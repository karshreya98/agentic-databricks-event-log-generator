#!/bin/bash
# Process Mining Toolkit — one-command setup
# Installs all prerequisites and configures the environment.

set -e

echo "=================================="
echo "Process Mining Toolkit Setup"
echo "=================================="
echo ""

# ── Step 1: Check Claude Code ──
if command -v claude &> /dev/null; then
    echo "[OK] Claude Code: $(claude --version 2>/dev/null || echo 'installed')"
else
    echo "[!!] Claude Code not found."
    echo "     Install: npm install -g @anthropic-ai/claude-code"
    echo "     Docs:    https://docs.anthropic.com/en/docs/claude-code"
    exit 1
fi

# ── Step 2: Check Databricks CLI ──
if command -v databricks &> /dev/null; then
    echo "[OK] Databricks CLI: $(databricks --version 2>/dev/null | head -1)"
else
    echo "[!!] Databricks CLI not found."
    echo "     Install: brew tap databricks/tap && brew install databricks"
    echo "     Or:      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
    exit 1
fi

# ── Step 3: Check Databricks auth ──
if databricks current-user me &> /dev/null; then
    USER=$(databricks current-user me 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('userName','unknown'))" 2>/dev/null || echo "authenticated")
    echo "[OK] Databricks auth: $USER"
else
    echo "[!!] Databricks not authenticated."
    echo ""
    read -p "     Enter your workspace URL (e.g., https://my-workspace.cloud.databricks.com): " WORKSPACE_URL
    if [ -n "$WORKSPACE_URL" ]; then
        echo "     Opening browser for authentication..."
        databricks auth login --host "$WORKSPACE_URL"
    else
        echo "     Run: databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com"
        exit 1
    fi
fi

# ── Step 4: Check/install AI Dev Kit ──
# The AI Dev Kit provides MCP tools (execute_sql, get_table_details, etc.)
# Check if it's already installed by looking for the plugin cache
AI_DEV_KIT_PATH="$HOME/.claude/plugins/cache/fe-vibe/databricks-ai-dev-kit"
if [ -d "$AI_DEV_KIT_PATH" ]; then
    VERSION=$(ls "$AI_DEV_KIT_PATH" 2>/dev/null | head -1)
    echo "[OK] Databricks AI Dev Kit: v$VERSION"
else
    echo "[!!] Databricks AI Dev Kit not found."
    echo "     Installing..."
    bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
    echo ""
    echo "[OK] AI Dev Kit installed. Restart Claude Code for tools to load."
fi

echo ""
echo "=================================="
echo "Setup complete!"
echo "=================================="
echo ""
echo "Next steps:"
echo "  1. Restart Claude Code (if AI Dev Kit was just installed)"
echo "  2. cd $(pwd)"
echo "  3. claude"
echo "  4. Type: /discover-event-log"
echo ""
