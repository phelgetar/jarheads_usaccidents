#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/backup_mysql.sh
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
# File: scripts/backup_mysql.sh
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Make a timestamped MySQL dump (macOS/BSD compatible date).
#   Requires DATABASE_URL in .env or explicit env vars:
#     MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
#
# Version: 0.4.1
#
# Revision History
# - 0.1.2 (2025-09-29): Use date "+%Y-%m-%d" (BSD/macOS); tighten perms — Tim Canady
# - 0.1.1 (2025-09-28): Add gzip & rotation — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#

set -euo pipefail

# Load .env for local runs (optional; safe because we only read simple KEY=VALUE)
if [ -f ".env" ]; then
  set -a; . ./.env; set +a
fi

# Parse from DATABASE_URL if present (format: mysql+pymysql://user:pass@host:port/db)
if [[ -n "${DATABASE_URL:-}" ]]; then
  proto_removed="${DATABASE_URL#*://}"                   # user:pass@host:port/db
  creds="${proto_removed%@*}"                             # user:pass
  hostdb="${proto_removed#*@}"                            # host:port/db
  user="${creds%%:*}"
  pass="${creds#*:}"
  host="${hostdb%%:*}"
  rest="${hostdb#*:}"                                     # port/db
  port="${rest%%/*}"
  db="${rest#*/}"
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

BACKUP_ROOT="/var/backups/usaccidents"
DAILY_DIR="$BACKUP_ROOT/daily"
mkdir -p "$DAILY_DIR"

STAMP="$(date "+%Y-%m-%d")"
OUT="$DAILY_DIR/${MYSQL_DB}_${STAMP}.sql.gz"

echo "[] Starting backup for ${MYSQL_DB} -> $OUT"
mysqldump --host="$MYSQL_HOST" --port="$MYSQL_PORT" \
  --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" \
  --single-transaction --routines --events --triggers \
  "$MYSQL_DB" | gzip -c > "$OUT"

chmod 640 "$OUT"
echo "[✓] Backup complete: $OUT"

# Optional: Keep last 14 backups
ls -1t "$DAILY_DIR"/*.sql.gz 2>/dev/null | tail -n +15 | xargs -I {} rm -f "{}" || true
