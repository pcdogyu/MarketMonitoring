#!/usr/bin/env bash
set -euo pipefail
VENV="mmenv"
if [[ ! -d "$VENV" ]]; then
  python3 -m venv "$VENV"
fi
source "$VENV/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip freeze > requirements.lock
echo "Locked packages to requirements.lock"

