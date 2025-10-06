#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: run_local.sh
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
set -a
. ./.env
set +a

exec uvicorn usaccidents_app.main:app --reload
