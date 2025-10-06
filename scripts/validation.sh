#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: scripts/validation.sh
# Purpose: Shell utility script.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.1.1 (2025-09-30): Fix scheduler bug
###################################################################
#
mkdir -p .githooks
cat > .githooks/pre-commit <<'EOF'
set -euo pipefail
# Run header validator for staged files
python scripts/validate_headers.py
EOF
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
