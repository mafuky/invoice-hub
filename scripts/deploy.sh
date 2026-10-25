#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
invhub_ssh_args
[[ "${INVHUB_REMOTE_DIR:-}" == /opt/invoice-hub ]] || {
  echo 'INVHUB_REMOTE_DIR must be /opt/invoice-hub.' >&2; exit 1;
}
remote_compose='cd /opt/invoice-hub && docker compose --project-name invoice-hub --env-file middleware.env -f compose.yaml'
case "${1:-}" in
  up)
    keys=(INVHUB_MYSQL_ROOT_PASSWORD INVHUB_MYSQL_APP_PASSWORD INVHUB_REDIS_PASSWORD
      INVHUB_RABBITMQ_PASSWORD INVHUB_MONGO_ROOT_PASSWORD INVHUB_MONGO_APP_PASSWORD INVHUB_MONGO_KEYFILE)
    for key in "${keys[@]}"; do
      [[ "${!key:-}" =~ ^[a-fA-F0-9]{32,}$ ]] || {
        echo "Expected a random hexadecimal secret in $key." >&2; exit 1;
      }
    done
    ssh "${SSH_ARGS[@]}" 'umask 077; mkdir -p /opt/invoice-hub; chmod 700 /opt/invoice-hub'
    tar --format ustar -C "$INVHUB_ROOT/deploy" -cf - . | ssh "${SSH_ARGS[@]}" 'tar -xf - -C /opt/invoice-hub'
    for key in "${keys[@]}"; do printf '%s=%s\n' "$key" "${!key}"; done |
      ssh "${SSH_ARGS[@]}" 'umask 077; cat > /opt/invoice-hub/middleware.env.tmp; chmod 600 /opt/invoice-hub/middleware.env.tmp; mv /opt/invoice-hub/middleware.env.tmp /opt/invoice-hub/middleware.env'
    ssh "${SSH_ARGS[@]}" "$remote_compose up -d --wait --wait-timeout 300"
    ;;
  down) ssh "${SSH_ARGS[@]}" "$remote_compose down" ;;
  ps) ssh "${SSH_ARGS[@]}" "$remote_compose ps" ;;
  *) echo 'Usage: scripts/deploy.sh up|down|ps' >&2; exit 2 ;;
esac
