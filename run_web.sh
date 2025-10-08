#!/usr/bin/env bash
#
###################################################################
# Project: USAccidents
# File: run_web.sh
# Purpose: Launch FastAPI app on localhost:8080 with web UI enabled.
#
# Description of code and how it works:
# - Loads .env into the environment and runs uvicorn on port 8080.
#
# Author: Tim Canady
# Created: 2025-10-07
#
# Version: 0.7.0
# Last Modified: 2025-10-07 by Tim Canady
#
# Revision History:
# - 0.7.0 (2025-10-07): Initial script.
###################################################################
#
set -euo pipefail
set -a
[ -f .env ] && . ./.env || true
set +a

exec uvicorn usaccidents_app.main:app --host 0.0.0.0 --port 8080 --reload
