#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/backup_files.sh
# Purpose: FastAPI usaccidents_app to ingest, normalize, and serve incidents.
#
# Author: Tim Canady
# Created: 2025-10-04
#
# Description:
#   Create compressed filesystem backups of the project directory.
#   Rotates daily and snapshot backups, keeping only N most recent.
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.1.0 (2025-10-06): Initial version — Tim Canady
###################################################################
#

set -euo pipefail

# === Config ===
BACKUP_ROOT="/var/backups/usaccidents/files"
SOURCE_DIR="${1:-$PWD}"        # directory to back up (default: current project)
KEEP_DAILY=7                   # keep last 7 daily backups
KEEP_SNAPSHOTS=10              # keep last 10 snapshot archives

DATESTAMP="$(date -I)"         # YYYY-MM-DD
DAILY_DIR="$BACKUP_ROOT/daily"
SNAPSHOT_DIR="$BACKUP_ROOT/snapshots"

mkdir -p "$DAILY_DIR" "$SNAPSHOT_DIR"

# === Step 1: Create daily archive ===
ARCHIVE_NAME="files_${DATESTAMP}.tar.gz"
ARCHIVE_PATH="$DAILY_DIR/$ARCHIVE_NAME"

echo "[i] Backing up $SOURCE_DIR → $ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$(dirname "$SOURCE_DIR")" "$(basename "$SOURCE_DIR")"

# === Step 2: Maintain rotation ===
echo "[i] Cleaning old daily backups (keeping $KEEP_DAILY)"
ls -1t "$DAILY_DIR"/*.tar.gz | tail -n +$((KEEP_DAILY+1)) | xargs -r rm -f

# === Step 3: Optionally snapshot today's backup ===
SNAPSHOT_NAME="snapshot_${DATESTAMP}.tar.gz"
SNAPSHOT_PATH="$SNAPSHOT_DIR/$SNAPSHOT_NAME"

if [[ ! -f "$SNAPSHOT_PATH" ]]; then
  echo "[i] Creating snapshot $SNAPSHOT_PATH"
  cp "$ARCHIVE_PATH" "$SNAPSHOT_PATH"
fi

echo "[i] Cleaning old snapshots (keeping $KEEP_SNAPSHOTS)"
ls -1t "$SNAPSHOT_DIR"/*.tar.gz | tail -n +$((KEEP_SNAPSHOTS+1)) | xargs -r rm -f

echo "[✓] File backup complete → $ARCHIVE_PATH"
