#!/bin/bash
# Install the discover-event-log skill into your Databricks workspace for Genie Code.
#
# Usage:
#   ./install-genie-code.sh                          # uses DEFAULT profile
#   ./install-genie-code.sh --profile my-workspace   # uses a specific profile
#
# This copies the skill to /Workspace/Users/<you>/.assistant/skills/discover-event-log/
# so Genie Code (Agent mode) can use it via @discover-event-log.

set -e

PROFILE_FLAG=""
if [ "$1" = "--profile" ] && [ -n "$2" ]; then
    PROFILE_FLAG="--profile $2"
    echo "Using Databricks profile: $2"
fi

# Get current user
USER=$(databricks $PROFILE_FLAG current-user me 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])" 2>/dev/null)
if [ -z "$USER" ]; then
    echo "Error: Could not determine Databricks user. Run: databricks auth login"
    exit 1
fi

SKILL_PATH="/Workspace/Users/${USER}/.assistant/skills/discover-event-log"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_SOURCE="${SCRIPT_DIR}/genie-code/discover-event-log"

echo "Installing discover-event-log skill for Genie Code..."
echo "  User:   ${USER}"
echo "  Target: ${SKILL_PATH}"

# Create directory and upload
databricks $PROFILE_FLAG workspace mkdirs "${SKILL_PATH}" 2>/dev/null
databricks $PROFILE_FLAG workspace import "${SKILL_PATH}/SKILL.md" \
    --file "${SKILL_SOURCE}/SKILL.md" \
    --format AUTO --overwrite

echo ""
echo "Done! Skill installed at ${SKILL_PATH}"
echo ""
echo "To use it:"
echo "  1. Open Genie Code in your workspace"
echo "  2. Switch to Agent mode"
echo "  3. Type: @discover-event-log Build an event log from <catalog>.<schema>"
echo ""
