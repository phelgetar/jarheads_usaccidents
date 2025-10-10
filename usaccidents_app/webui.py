#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/webui.py
# Purpose: Simple web UI (incidents + logs viewer)
#
# Description of code and how it works:
# - /web/incidents: filters UI
# - /web/logs: live log tail viewer
#
# Author: Tim Canady
# Created: 2025-10-08
#
# Version: 0.4.0
# Last Modified: 2025-10-09 by Tim Canady
#
# Revision History:
# - 0.4.0 (2025-10-09): Add /web/logs route.
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
    return templates.TemplateResponse("incidents.html", {"request": request, "title": "USAccidents – Incidents"})

@router.get("/logs")
async def ui_logs(request: Request):
    return templates.TemplateResponse("logs.html", {"request": request, "title": "USAccidents – Logs"})
