#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/restore_files.sh
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
# File: scripts/restore_files.sh
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Restore project files or directories from compressed backups.
#   Supports restoring entire archive or a specific directory/file.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.0 (2025-09-30): Initial — Tim Canady
###################################################################
#

set -euo pipefail

BACKUP_FILE="${1:-}"
TARGET_PATH="${2:-}"

if [[ -z "$BACKUP_FILE" ]]; then
  echo "Usage: $0 <backup.tar.gz> [target_subpath]"
  echo "Examples:"
  echo "  $0 /var/backups/usaccidents/snapshots/files_2025-09-30.tar.gz"
  echo "  $0 /var/backups/usaccidents/snapshots/files_2025-09-30.tar.gz usaccidents_app/main.py"
  echo "  $0 /var/backups/usaccidents/snapshots/files_2025-09-30.tar.gz usaccidents_app/"
  exit 1
fi

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "Backup file not found: $BACKUP_FILE"
  exit 2
fi

echo "[i] Restoring from $BACKUP_FILE"

if [[ -n "$TARGET_PATH" ]]; then
  echo "[i] Restoring only $TARGET_PATH"
  tar -xvzf "$BACKUP_FILE" "$TARGET_PATH"
else
  echo "[i] Restoring entire archive"
  tar -xvzf "$BACKUP_FILE"
fi

echo "[✓] Restore complete."
