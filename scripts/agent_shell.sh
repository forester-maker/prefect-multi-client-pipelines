#!/usr/bin/env bash
# Open a Claude Code session that acts as the pipelines service account:
# the Prefect MCP server (reads) and the prefect CLI (run / cancel) both pick up
# PREFECT_API_URL / PREFECT_API_KEY from the environment. Same key, same role,
# same audit trail as any app would have.
cd "$(dirname "$0")/.."
set -a; source .env; set +a
export PATH="$PWD/.venv/bin:$PATH"
exec claude "$@"
