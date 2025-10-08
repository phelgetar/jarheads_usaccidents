#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/webui.py
# Purpose: Simple web UI router + API for incidents with filters.
#
# Description of code and how it works:
# - Serves /web/incidents (HTML) and /api/incidents (JSON).
# - Filters: event_type, state/location search (route/county/state), direction, status (active|cleared|any).
#
# Author: Tim Canady
# Created: 2025-10-07
#
# Version: 0.7.0
# Last Modified: 2025-10-07 by Tim Canady
#
# Revision History:
# - 0.7.0 (2025-10-07): Initial web UI with filters and JSON endpoint.
###################################################################
#
#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/webui.py
# Purpose: Simple web UI for incidents with live active count
#
# Description of code and how it works:
# - Serves /web/incidents template.
# - Header shows live active incident count, refreshed by JS every 30s.
#
# Author: Tim Canady
# Created: 2025-10-08
#
# Version: 0.2.0
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.2.0 (2025-10-08): Add active count display & polling.
# - 0.1.0 (2025-10-08): Initial UI router.
###################################################################
#
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pathlib import Path

router = APIRouter(prefix="/web", tags=["web"])

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

@router.get("/incidents")
async def ui_incidents(request: Request):
    return templates.TemplateResponse(
        "incidents.html",
        {"request": request, "title": "USAccidents – Incidents"}
    )
