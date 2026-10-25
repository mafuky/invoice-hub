#!/usr/bin/env bash

INVHUB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -f "$INVHUB_ROOT/.env" ]]; then
  echo 'Missing .env; use .env.example to configure the environment.' >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source "$INVHUB_ROOT/.env"
set +a

invhub_ssh_args() {
  : "${INVHUB_SSH_HOST:?missing INVHUB_SSH_HOST}"
  : "${INVHUB_SSH_USER:?missing INVHUB_SSH_USER}"
  : "${INVHUB_SSH_PORT:?missing INVHUB_SSH_PORT}"
  SSH_ARGS=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15
    -o ServerAliveCountMax=3 -o ControlMaster=no -o ControlPath=none
    -p "$INVHUB_SSH_PORT" "$INVHUB_SSH_USER@$INVHUB_SSH_HOST")
}
