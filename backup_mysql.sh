#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: backup_mysql.sh
# Purpose: Shell utility script.
#
# Description of code and how it works:
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.1.1 (2025-09-28): Connector refactor + DB tuning
###################################################################
#
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL must be set}"
BACKUP_DIR=/var/backups/usaccidents
mkdir -p "$BACKUP_DIR"

# Parse DATABASE_URL (mysql+pymysql://user:pass@host:port/db?params)
proto_removed=${DATABASE_URL#mysql+pymysql://}
creds_host=${proto_removed%%/*}
dbname=${proto_removed#*/}
dbname=${dbname%%\?*}
userpass=${creds_host%@*}
hostport=${creds_host#*@}
user=${userpass%%:*}
pass=${userpass#*:}
host=${hostport%%:*}
port=${hostport#*:}

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mysqldump -h "$host" -P "${port:-3306}" -u "$user" -p"$pass" "$dbname"   | gzip > "$BACKUP_DIR/${dbname}_${TIMESTAMP}.sql.gz"

echo "Backup created: $BACKUP_DIR/${dbname}_${TIMESTAMP}.sql.gz"
