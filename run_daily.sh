#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/home/kemura/projects/meteora-watch"
cd "$REPO_DIR"

PY="python3"
if [[ -x "$REPO_DIR/.venv/bin/python" ]]; then
  PY="$REPO_DIR/.venv/bin/python"
fi

"$PY" "$REPO_DIR/meteora_watch.py" collect
