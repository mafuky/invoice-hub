#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
cd "$INVHUB_ROOT/tools/devcheck"
export GOCACHE="${GOCACHE:-$INVHUB_ROOT/runs/go-cache}"
export GOWORK=off
exec go run .
