#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/main.py
# Purpose: FastAPI app (API + web UI + scheduler)
#
# Description of code and how it works:
# - Serves the Incidents web UI and a live log viewer.
# - Provides incidents search/facets/active_count plus latest/changed_since.
# - Implements MySQL-safe ordering that emulates "NULLS LAST".
# - Includes OHGO + DriveTexas ingest endpoints and a scheduler.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.12.0
# Last Modified: 2025-10-14 by Tim Canady
#
# Revision History:
# - 0.12.0 (2025-10-14): Added DriveTexas (TX) endpoints + optional scheduler job.
# - 0.11.1 (2025-10-09): Restore simple ORM /incidents/search; keep UI + logs + scheduler stable.
# - 0.11.0 (2025-10-09): Added robust ordering helpers; expanded UI endpoints.
###################################################################
#

from __future__ import annotations

import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Depends, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from .database import get_db
from .models import Incident
from .connectors.ohio import (
    fetch_ohgo_incidents,
    ingest_ohgo_incidents,
    fetch_ohgo_all,
    fetch_ohgo_roads,
    ingest_ohgo_roads,
)
from .connectors.texas import (
    fetch_texas_incidents,
    ingest_texas_incidents,
)

# ------------------------------------------------------------------------------
# Logging (shared with connectors + log viewer)
# ------------------------------------------------------------------------------

LOG_FILE = os.getenv(
    "USACCIDENTS_LOG_FILE",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "usaccidents.log"),
)

log = logging.getLogger("usaccidents")
log.setLevel(logging.INFO)

if not log.handlers:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    sh = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S%z")
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)

# ------------------------------------------------------------------------------
# FastAPI / static / templates
# ------------------------------------------------------------------------------

app = FastAPI(title="USAccidents")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
STATIC_DIR = os.path.join(ROOT_DIR, "static")
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# ------------------------------------------------------------------------------
# Root -> Incidents UI
# ------------------------------------------------------------------------------

@app.get("/")
async def _root():
    # Default to the incidents page
    return RedirectResponse(url="/web/incidents", status_code=302)

@app.get("/web/incidents", response_class=HTMLResponse)
async def web_incidents(request: Request):
    return templates.TemplateResponse("incidents.html", {"request": request, "title": "USAccidents – Incidents"})

# ------------------------------------------------------------------------------
# Simple Logs viewer
# ------------------------------------------------------------------------------

@app.get("/web/logs", response_class=HTMLResponse)
async def web_logs(request: Request):
    html = """<!doctype html><html><head><meta charset="utf-8"><title>Logs</title>
<link rel="stylesheet" href="/static/styles.css"/></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif">
<header style="padding:12px 16px; border-bottom:1px solid #e5e7eb; display:flex; justify-content:space-between; align-items:center;">
  <h2 style="margin:0">Application Logs</h2>
  <nav><a href="/web/incidents">Incidents</a></nav>
</header>
<pre id="logbox" style="white-space:pre-wrap; border-top:1px solid #e5e7eb; padding:12px; margin:0; height:80vh; overflow:auto"></pre>
<script src="/static/logs.js"></script>
</body></html>"""
    return HTMLResponse(html)

@app.get("/logs/tail", response_class=PlainTextResponse)
async def logs_tail(lines: int = 500):
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            data = f.readlines()[-max(1, min(lines, 5000)):]
        return PlainTextResponse("".join(data))
    except FileNotFoundError:
        return PlainTextResponse("[no log file yet]\n")

# ------------------------------------------------------------------------------
# Utility: MySQL-safe ordering ("NULLS LAST")
# ------------------------------------------------------------------------------

def _nulls_last_desc(col):
    # ORDER BY (col IS NULL) ASC, col DESC
    return (col.is_(None).asc(), col.desc())

# ------------------------------------------------------------------------------
# Incidents endpoints (web UI needs these)
# ------------------------------------------------------------------------------

@app.get("/incidents/active_count")
def incidents_active_count(db: Session = Depends(get_db)):
    # Active = is_active is true OR (is_active is null and cleared_time is null)
    active_expr = or_(
        Incident.is_active.is_(True),
        and_(Incident.is_active.is_(None), Incident.cleared_time.is_(None)),
    )
    total = db.query(Incident).filter(active_expr).count()
    return {"active_count": int(total)}

@app.get("/incidents/facets")
def incidents_facets(db: Session = Depends(get_db)):
    def facet(col):
        vals = (
            db.query(col)
            .filter(col.isnot(None))
            .distinct()
            .order_by(col.asc())
            .all()
        )
        return [v[0] for v in vals if v[0] is not None]

    return {
        "state": facet(Incident.state),
        "county": facet(Incident.county),
        "route": facet(Incident.route),
        "route_class": facet(Incident.route_class),
        "direction": facet(Incident.direction),
        "event_type": facet(Incident.event_type),
        "closure_status": facet(Incident.closure_status),
        "severity_flag": facet(Incident.severity_flag),
    }

@app.get("/incidents/search")
def incidents_search(
    db: Session = Depends(get_db),
    # multi-value filters
    state: List[str] = Query(default=[]),
    county: List[str] = Query(default=[]),
    route: List[str] = Query(default=[]),
    route_class: List[str] = Query(default=[]),
    direction: List[str] = Query(default=[]),
    event_type: List[str] = Query(default=[]),
    closure_status: List[str] = Query(default=[]),
    severity_flag: List[str] = Query(default=[]),
    # single-value filters
    is_active: Optional[bool] = Query(default=None),
    active_only: Optional[bool] = Query(default=None),
    severity_score_min: Optional[int] = Query(default=None, ge=0, le=3),
    severity_score_max: Optional[int] = Query(default=None, ge=0, le=3),
    updated_since: Optional[str] = Query(default=None),   # ISO
    reported_since: Optional[str] = Query(default=None),  # ISO
    order: str = Query(default="updated_time_desc"),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    q = db.query(Incident)

    def parse_iso(s: Optional[str]):
        if not s: return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    if state:          q = q.filter(Incident.state.in_(state))
    if county:         q = q.filter(Incident.county.in_(county))
    if route:          q = q.filter(Incident.route.in_(route))
    if route_class:    q = q.filter(Incident.route_class.in_(route_class))
    if direction:      q = q.filter(Incident.direction.in_(direction))
    if event_type:     q = q.filter(Incident.event_type.in_(event_type))
    if closure_status: q = q.filter(Incident.closure_status.in_(closure_status))
    if severity_flag:  q = q.filter(Incident.severity_flag.in_(severity_flag))

    if active_only or (is_active is True):
        q = q.filter(
            or_(
                Incident.is_active.is_(True),
                and_(Incident.is_active.is_(None), Incident.cleared_time.is_(None)),
            )
        )
    elif is_active is False:
        q = q.filter(
            or_(
                Incident.is_active.is_(False),
                and_(Incident.is_active.is_(None), Incident.cleared_time.is_not(None)),
            )
        )

    if severity_score_min is not None:
        q = q.filter(Incident.severity_score >= severity_score_min)
    if severity_score_max is not None:
        q = q.filter(Incident.severity_score <= severity_score_max)

    if updated_since:
        dt = parse_iso(updated_since)
        if dt: q = q.filter(Incident.updated_time >= dt)
    if reported_since:
        dt = parse_iso(reported_since)
        if dt: q = q.filter(Incident.reported_time >= dt)

    total = q.count()

    if order == "reported_time_desc":
        q = q.order_by(*_nulls_last_desc(Incident.reported_time))
    elif order == "severity_desc":
        q = q.order_by(*_nulls_last_desc(Incident.severity_score),
                       *_nulls_last_desc(Incident.updated_time))
    else:
        q = q.order_by(*_nulls_last_desc(Incident.updated_time),
                       *_nulls_last_desc(Incident.reported_time))

    rows = q.limit(limit).offset(offset).all()

    def to_obj(x: Incident) -> Dict[str, Any]:
        return {
            "uuid": x.uuid,
            "source_system": x.source_system,
            "source_event_id": x.source_event_id,
            "state": x.state,
            "county": x.county,
            "route": x.route,
            "route_class": x.route_class,
            "direction": x.direction,
            "latitude": float(x.latitude) if x.latitude is not None else None,
            "longitude": float(x.longitude) if x.longitude is not None else None,
            "reported_time": x.reported_time.isoformat() if x.reported_time else None,
            "updated_time": x.updated_time.isoformat() if x.updated_time else None,
            "cleared_time": x.cleared_time.isoformat() if x.cleared_time else None,
            "is_active": x.is_active,
            "event_type": x.event_type,
            "lanes_affected": x.lanes_affected,
            "closure_status": x.closure_status,
            "severity_flag": x.severity_flag,
            "severity_score": int(x.severity_score) if x.severity_score is not None else None,
            "source_url": x.source_url,
        }

    items = [to_obj(r) for r in rows]
    return {"ok": True, "total": int(total), "count": len(items), "items": items}

# ------------------------------------------------------------------------------
# Additional incidents endpoints (as per original spec)
# ------------------------------------------------------------------------------

@app.get("/incidents/latest")
def incidents_latest(
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=500),
):
    q = db.query(Incident).order_by(
        *_nulls_last_desc(Incident.updated_time),
        *_nulls_last_desc(Incident.reported_time),
    )
    rows = q.limit(limit).all()
    return {"ok": True, "count": len(rows), "items": [
        {
            "uuid": x.uuid,
            "route": x.route,
            "direction": x.direction,
            "event_type": x.event_type,
            "closure_status": x.closure_status,
            "severity_score": int(x.severity_score) if x.severity_score is not None else None,
            "is_active": x.is_active,
            "reported_time": x.reported_time.isoformat() if x.reported_time else None,
            "updated_time": x.updated_time.isoformat() if x.updated_time else None,
        } for x in rows
    ]}

@app.get("/incidents/changed_since")
def incidents_changed_since(
    since: str = Query(..., description="ISO timestamp, e.g. 2025-10-01T00:00:00Z"),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    try:
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid 'since' ISO datetime"}, status_code=400)

    q = db.query(Incident).filter(
        or_(
            Incident.updated_time >= dt,
            and_(Incident.updated_time.is_(None), Incident.reported_time >= dt),
        )
    ).order_by(
        *_nulls_last_desc(Incident.updated_time),
        *_nulls_last_desc(Incident.reported_time),
    )
    rows = q.limit(limit).all()
    return {"ok": True, "count": len(rows), "items": [
        {
            "uuid": x.uuid,
            "updated_time": x.updated_time.isoformat() if x.updated_time else None,
            "reported_time": x.reported_time.isoformat() if x.reported_time else None,
        } for x in rows
    ]}

# ------------------------------------------------------------------------------
# Health
# ------------------------------------------------------------------------------

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}

# ------------------------------------------------------------------------------
# OHGO ingest endpoints (existing)
# ------------------------------------------------------------------------------

@app.post("/ingest/ohio/fetch")
async def ingest_ohio_fetch(
    detail: bool = Query(default=False),
    page_all: bool = Query(default=True),
    region: Optional[str] = Query(default=None),
    bounds_sw: Optional[str] = Query(default=None),
    bounds_ne: Optional[str] = Query(default=None),
    radius: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    items = await fetch_ohgo_incidents(
        page_size=100,
        page_all=page_all,
        region=region,
        bounds_sw=bounds_sw,
        bounds_ne=bounds_ne,
        radius=radius,
    )
    result = ingest_ohgo_incidents(db, items, return_detail=detail)
    out = {"ok": True, "filters": {
        "page_all": page_all, "region": region,
        "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius
    }}
    if detail and isinstance(result, dict):
        out["result"] = result
    else:
        out["result"] = {"processed": int(result) if isinstance(result, int) else None}
    return JSONResponse(out)

@app.post("/ingest/ohio/roads")
async def ingest_ohio_roads(
    db: Session = Depends(get_db),
):
    items = await fetch_ohgo_roads()
    n = ingest_ohgo_roads(db, items)
    return {"ok": True, "processed": int(n)}

# ------------------------------------------------------------------------------
# DriveTexas ingest endpoints (new)
# ------------------------------------------------------------------------------

@app.post("/ingest/texas/fetch")
async def ingest_texas_fetch(
    detail: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    items = await fetch_texas_incidents()
    result = ingest_texas_incidents(db, items, return_detail=detail)
    return {"ok": True, "count": len(items), "result": result}

# ------------------------------------------------------------------------------
# Collector (optional helper)
# ------------------------------------------------------------------------------

@app.get("/collect/ohio")
async def collect_ohio(
    detail: bool = Query(default=False),
    page_all: bool = Query(default=True),
    region: Optional[str] = Query(default=None),
    bounds_sw: Optional[str] = Query(default=None),
    bounds_ne: Optional[str] = Query(default=None),
    radius: Optional[str] = Query(default=None),
    include: List[str] = Query(default=[]),
):
    data = await fetch_ohgo_all(
        page_all=page_all,
        region=region,
        bounds_sw=bounds_sw,
        bounds_ne=bounds_ne,
        radius=radius,
        include=include or None,  # incidents only by default
    )
    counts = {}
    for k, v in data.items():
        if isinstance(v, list):
            counts[k] = len(v)
        else:
            counts[k] = (v.get("type", "object") if isinstance(v, dict) else type(v).__name__)
    return {"ok": True, "counts": counts, **data}

# ------------------------------------------------------------------------------
# Scheduler
# ------------------------------------------------------------------------------

scheduler = AsyncIOScheduler()

async def _scheduled_ohio_ingest():
    try:
        log.info("[SYNC] scheduler_start source=OHGO ts=%s", datetime.utcnow().isoformat() + "Z")
        items = await fetch_ohgo_incidents(page_all=True)
        from .database import SessionLocal
        db = SessionLocal()
        try:
            result = ingest_ohgo_incidents(db, items, return_detail=True)
        finally:
            db.close()
        log.info("[SYNC] scheduler_success source=OHGO inserted=%s updated=%s skipped=%s",
                 result.get("inserted"), result.get("updated"), result.get("skipped"))
    except Exception as e:
        log.exception("[SYNC] scheduler_error source=OHGO err=%s", e)

async def _scheduled_texas_ingest():
    try:
        log.info("[SYNC] scheduler_start source=DRIVETEXAS ts=%s", datetime.utcnow().isoformat() + "Z")
        items = await fetch_texas_incidents()
        from .database import SessionLocal
        db = SessionLocal()
        try:
            result = ingest_texas_incidents(db, items, return_detail=True)
        finally:
            db.close()
        log.info("[SYNC] scheduler_success source=DRIVETEXAS inserted=%s updated=%s skipped=%s",
                 result.get("inserted"), result.get("updated"), result.get("skipped"))
    except Exception as e:
        log.exception("[SYNC] scheduler_error source=DRIVETEXAS err=%s", e)

@app.on_event("startup")
async def _startup():
    # Every 1 minute: OHGO; every 2 minutes: DriveTexas
    scheduler.add_job(_scheduled_ohio_ingest, "interval", minutes=1, id="ohio_ingest")
    scheduler.add_job(_scheduled_texas_ingest, "interval", minutes=2, id="texas_ingest")
    scheduler.start()

@app.on_event("shutdown")
async def _shutdown():
    scheduler.shutdown(wait=False)
