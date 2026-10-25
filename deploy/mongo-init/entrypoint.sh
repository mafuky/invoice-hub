#!/usr/bin/env bash
set -euo pipefail
umask 077
printf '%s\n' "$MONGO_KEYFILE_CONTENT" > /data/keyfile/key
chown mongodb:mongodb /data/keyfile/key
chmod 400 /data/keyfile/key
exec /usr/local/bin/docker-entrypoint.sh "$@"
