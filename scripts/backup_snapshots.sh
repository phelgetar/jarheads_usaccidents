#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/backup_snapshots.sh
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
# File: scripts/backup_snapshots.sh
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Create a compressed DB snapshot in a secondary location, e.g. weekly.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.0 (2025-09-30): Initial — Tim Canady
###################################################################
#

set -euo pipefail
if [ -f ".env" ]; then set -a; . ./.env; set +a; fi

# Reuse daily dump if exists
SRC_DIR="/var/backups/usaccidents/daily"
DST_DIR="/var/backups/usaccidents/snapshots"
mkdir -p "$DST_DIR"

LATEST="$(ls -1t "$SRC_DIR"/*.sql.gz | head -n1)"
: "${LATEST:?No daily backups in $SRC_DIR}"

STAMP="$(date "+%Y-%m-%d")"
OUT="$DST_DIR/snapshot_${STAMP}.sql.gz"

cp -p "$LATEST" "$OUT"
chmod 640 "$OUT"
echo "[✓] Snapshot copied to $OUT"

# Keep last 8 snapshots
ls -1t "$DST_DIR"/*.sql.gz 2>/dev/null | tail -n +9 | xargs -I {} rm -f "{}" || true
