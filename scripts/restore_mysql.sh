#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/restore_mysql.sh
# Purpose: FastAPI usaccidents_app to ingest, normalize, and serve incidents.
#
# Author: Tim Canady
# Created: 2025-10-04
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.4.0 (2025-10-04): Initial version — Tim Canady
###################################################################
#
#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: scripts/restore_mysql.sh
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Restore the latest gzip’d backup into the configured DB.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.2 (2025-09-29): Safer selection; clear prompts — Tim Canady
# - 0.1.1 (2025-09-28): Add .env parsing — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#

set -euo pipefail
if [ -f ".env" ]; then set -a; . ./.env; set +a; fi

# Pull from DATABASE_URL if present
if [[ -n "${DATABASE_URL:-}" ]]; then
  proto_removed="${DATABASE_URL#*://}"
  creds="${proto_removed%@*}"
  hostdb="${proto_removed#*@}"
  user="${creds%%:*}"; pass="${creds#*:}"
  host="${hostdb%%:*}"; rest="${hostdb#*:}"
  port="${rest%%/*}"; db="${rest#*/}"
  MYSQL_HOST="${MYSQL_HOST:-$host}"
  MYSQL_PORT="${MYSQL_PORT:-$port}"
  MYSQL_USER="${MYSQL_USER:-$user}"
  MYSQL_PASSWORD="${MYSQL_PASSWORD:-$pass}"
  MYSQL_DB="${MYSQL_DB:-$db}"
fi

: "${MYSQL_HOST:=localhost}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_USER:?MYSQL_USER required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD required}"
: "${MYSQL_DB:?MYSQL_DB required}"

BACKUP_ROOT="/var/backups/usaccidents/daily"
LATEST="$(ls -1t "$BACKUP_ROOT"/*.sql.gz | head -n1)"
: "${LATEST:?No backups found in $BACKUP_ROOT}"

echo "Restoring $LATEST into $MYSQL_DB on $MYSQL_HOST:$MYSQL_PORT … (Ctrl+C to abort)"
sleep 2
gunzip -c "$LATEST" | mysql \
  --host="$MYSQL_HOST" --port="$MYSQL_PORT" \
  --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" \
  "$MYSQL_DB"
echo "[✓] Restore complete."
