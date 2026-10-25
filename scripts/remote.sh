#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
[[ $# -gt 0 ]] || { echo 'Usage: scripts/remote.sh <command...>' >&2; exit 2; }
invhub_ssh_args
if [[ $# == 1 ]]; then
  exec ssh "${SSH_ARGS[@]}" "$1"
fi
printf -v command '%q ' "$@"
exec ssh "${SSH_ARGS[@]}" "$command"
