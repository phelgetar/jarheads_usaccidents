#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/dry-run.sh
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
# File: scripts/dry-run.sh
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Quick smoke checks: headers + versions.yml presence.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.1 (2025-09-30): Validate headers for .py — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#

set -euo pipefail

if [ ! -f "versions.yml" ]; then
  echo "versions.yml missing. Run: python scripts/init_versions.py"
  exit 1
fi

python scripts/validate_headers.py usaccidents_app scripts --ext .py
echo "[✓] Dry run passed."
