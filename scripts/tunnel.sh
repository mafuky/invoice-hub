#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
exec python3 "$INVHUB_ROOT/scripts/tunnel.py" "${1:-status}"
