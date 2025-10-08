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
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import or_, func, case, desc
import os

from .database import get_db
from .models import Incident
from .schemas import IncidentOut

router = APIRouter()

TEMPLATES_DIR = os.getenv("USACCIDENTS_TEMPLATES", "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

def _mysql_nulls_last_desc(col):
    return (case((col.is_(None), 1), else_=0), desc(col))

@router.get("/web/incidents", response_class=HTMLResponse)
async def web_incidents(request: Request):
    return templates.TemplateResponse("incidents.html", {"request": request})

@router.get("/api/incidents", response_model=List[IncidentOut])
async def api_incidents(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=500),
    event_type: Optional[str] = Query(None),
    state: Optional[str] = Query(None, description="State code like OH"),
    location: Optional[str] = Query(None, description="Free-text search in route/county/state"),
    direction: Optional[str] = Query(None, description="e.g., Northbound, Southbound, Both Directions"),
    status: Optional[str] = Query("any", regex="^(any|active|cleared)$"),
):
    q = db.query(Incident)

    if event_type:
        q = q.filter(Incident.event_type == event_type)

    if state:
        q = q.filter(func.upper(Incident.state) == state.upper())

    if location:
        pattern = f"%{location.lower()}%"
        q = q.filter(or_(func.lower(Incident.route).like(pattern),
                         func.lower(Incident.county).like(pattern),
                         func.lower(Incident.state).like(pattern)))

    if direction:
        q = q.filter(Incident.direction == direction)

    if status == "active":
        # Prefer explicit flag; include uncleared
        q = q.filter(or_(Incident.is_active.is_(True), Incident.cleared_time.is_(None)))
    elif status == "cleared":
        q = q.filter(or_(Incident.is_active.is_(False), Incident.cleared_time.is_not(None)))

    q = q.order_by(*_mysql_nulls_last_desc(Incident.updated_time)).limit(limit)
    return q.all()
