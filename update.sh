#!/usr/bin/env bash
set -euo pipefail

# Update repository and restart semeru-bot service
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Pull latest code
if [ -d .git ]; then
    git pull --ff-only
fi

# Ensure virtual environment exists
if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Restart service if available
if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl restart semeru-bot || true
fi

echo "Update complete."
