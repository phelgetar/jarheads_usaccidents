#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/main.py
# Purpose: FastAPI app (incidents API, OHGO ingest, collector, active-count, filters, logging & log viewer API)
#
# Description of code and how it works:
# - Initializes rotating file logging on startup.
# - Logs all scheduler/manual ingests and any exceptions.
# - Adds /logs/tail and /logs/download (simple viewer backend).
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 1.2.0
# Last Modified: 2025-10-09 by Tim Canady
#
# Revision History:
# - 1.2.0 (2025-10-09): Logging & log viewer endpoints.
# - 1.1.0 (2025-10-09): Faceted filtering API for UI.
###################################################################
#
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.responses import RedirectResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import case, desc, asc, func, or_, and_
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pathlib import Path
from datetime import datetime
import os
import io
import logging

from .logging_config import setup_logging
from .database import get_db
from .models import Incident
from .schemas import IncidentOut
from .connectors.ohio import (
    fetch_ohgo_incidents,
    ingest_ohgo_incidents,
    fetch_ohgo_roads,
    ingest_ohgo_roads,
    fetch_ohgo_all,
)
from .connectors.texas import (
    fetch_texas_incidents,
    ingest_texas_incidents,
)


# Initialize logging early
logger = setup_logging()

app = FastAPI(title="usaccidents_app")
scheduler: Optional[AsyncIOScheduler] = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = os.getenv("USACCIDENTS_STATIC", str(PROJECT_ROOT / "static"))
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Optional web UI (if present)
try:
    from .webui import router as web_router  # type: ignore
except Exception:
    web_router = None
else:
    app.include_router(web_router)

@app.get("/")
async def _root():
    if web_router:
        return RedirectResponse(url="/web/incidents", status_code=302)
    return {"message": "USAccidents API"}

@app.on_event("startup")
async def _startup():
    global scheduler
    scheduler = AsyncIOScheduler()

    async def _scheduled_ohio_ingest():
        from .database import SessionLocal
        start = datetime.utcnow()
        logger.info("[SYNC] scheduler_start source=OHGO ts=%s", start.isoformat() + "Z")
        try:
            items = await fetch_ohgo_incidents(page_all=True)
            db = SessionLocal()
            try:
                result = ingest_ohgo_incidents(db, items, return_detail=True)
                logger.info("[SYNC] scheduler_success source=OHGO inserted=%d updated=%d skipped=%d duration_sec=%.3f",
                            result.get("inserted", 0), result.get("updated", 0), result.get("skipped", 0),
                            (datetime.utcnow() - start).total_seconds())
            finally:
                db.close()
        except Exception as e:
            logger.exception("[SYNC] scheduler_error source=OHGO err=%s", e)

    scheduler.add_job(_scheduled_ohio_ingest, "interval", minutes=1)  # direct coro
    scheduler.start()
    logger.info("Scheduler started (OHGO ingest every 1 minute).")

@app.on_event("shutdown")
async def _shutdown():
    if scheduler:
        scheduler.shutdown(wait=False)
    logger.info("Application shutdown.")

def _mysql_nulls_last_desc(col):
    return (case((col.is_(None), 1), else_=0), desc(col))

def _mysql_nulls_last_asc(col):
    return (case((col.is_(None), 1), else_=0), asc(col))

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/incidents/active_count")
async def incidents_active_count(db: Session = Depends(get_db)):
    cnt = (
        db.query(Incident)
          .filter(or_(Incident.is_active.is_(True),
                      and_(Incident.is_active.is_(None), Incident.cleared_time.is_(None))))
          .count()
    )
    return {"active_count": cnt}

@app.get("/incidents/latest", response_model=List[IncidentOut])
async def incidents_latest(
    limit: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(Incident).order_by(*_mysql_nulls_last_desc(Incident.reported_time)).limit(limit)
    return q.all()

@app.get("/incidents/changed_since", response_model=List[IncidentOut])
async def incidents_changed_since(
    since: Optional[str] = Query(None, description="ISO timestamp (e.g., 2025-10-01T00:00:00Z)"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = db.query(Incident)
    if since:
        dt = _parse_iso(since)
        if dt:
            q = q.filter(Incident.updated_time >= dt)
    q = q.order_by(*_mysql_nulls_last_desc(Incident.updated_time)).limit(limit)
    return q.all()

# ---------- facets & search (unchanged from prior step, kept for context) ----------
@app.get("/incidents/facets")
async def incidents_facets(db: Session = Depends(get_db)):
    def distinct_non_null(col):
        rows = (db.query(col).filter(col.isnot(None)).distinct().order_by(asc(col)).all())
        return [r[0] for r in rows if r[0] is not None]
    return {
        "state": distinct_non_null(Incident.state),
        "county": distinct_non_null(Incident.county),
        "route": distinct_non_null(Incident.route),
        "route_class": distinct_non_null(Incident.route_class),
        "direction": distinct_non_null(Incident.direction),
        "event_type": distinct_non_null(Incident.event_type),
        "closure_status": distinct_non_null(Incident.closure_status),
        "severity_flag": distinct_non_null(Incident.severity_flag),
        "is_active_choices": [True, False],
    }

@app.get("/incidents/search")
async def incidents_search(
    state: Optional[List[str]] = Query(None),
    county: Optional[List[str]] = Query(None),
    route: Optional[List[str]] = Query(None),
    route_class: Optional[List[str]] = Query(None),
    direction: Optional[List[str]] = Query(None),
    event_type: Optional[List[str]] = Query(None),
    closure_status: Optional[List[str]] = Query(None),
    severity_flag: Optional[List[str]] = Query(None),
    is_active: Optional[bool] = Query(None),
    active_only: Optional[bool] = Query(None),
    severity_score_min: Optional[int] = Query(None, ge=0),
    severity_score_max: Optional[int] = Query(None, ge=0),
    updated_since: Optional[str] = Query(None),
    reported_since: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    order: str = Query("updated_time_desc"),
    db: Session = Depends(get_db),
):
    q = db.query(Incident)
    conds = []
    def in_filter(col, values: Optional[List[str]]):
        if values:
            conds.append(col.in_(values))
    in_filter(Incident.state, state); in_filter(Incident.county, county); in_filter(Incident.route, route)
    in_filter(Incident.route_class, route_class); in_filter(Incident.direction, direction)
    in_filter(Incident.event_type, event_type); in_filter(Incident.closure_status, closure_status)
    in_filter(Incident.severity_flag, severity_flag)
    if active_only:
        conds.append(or_(Incident.is_active.is_(True),
                         and_(Incident.is_active.is_(None), Incident.cleared_time.is_(None))))
    elif is_active is not None:
        conds.append(Incident.is_active.is_(True) if is_active else Incident.is_active.is_(False))
    if severity_score_min is not None: conds.append(Incident.severity_score >= severity_score_min)
    if severity_score_max is not None: conds.append(Incident.severity_score <= severity_score_max)
    if updated_since:
        dt = _parse_iso(updated_since);
        if dt: conds.append(Incident.updated_time >= dt)
    if reported_since:
        dt = _parse_iso(reported_since);
        if dt: conds.append(Incident.reported_time >= dt)
    if conds: q = q.filter(and_(*conds))
    total = q.count()
    if order == "reported_time_desc":
        q = q.order_by(*_mysql_nulls_last_desc(Incident.reported_time))
    elif order == "severity_desc":
        q = q.order_by(case((Incident.severity_score.is_(None), 1), else_=0), desc(Incident.severity_score))
    else:
        q = q.order_by(*_mysql_nulls_last_desc(Incident.updated_time))
    q = q.offset(offset).limit(limit)
    items = q.all()
    return {"total": total, "count": len(items), "items": [IncidentOut.from_orm(x).dict() for x in items]}

# ---------- ingest & collect with logging ----------
@app.post("/ingest/ohio/fetch")
async def ingest_ohgo_fetch(
    db: Session = Depends(get_db),
    page_size: int = Query(500, ge=1, le=2000),
    page_all: bool = Query(True),
    region: Optional[str] = Query(None),
    bounds_sw: Optional[str] = Query(None),
    bounds_ne: Optional[str] = Query(None),
    radius: Optional[str] = Query(None),
    detail: bool = Query(True),
):
    start = datetime.utcnow()
    logger.info("[SYNC] manual_start source=OHGO params=%s", {
        "page_size": page_size, "page_all": page_all, "region": region,
        "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius
    })
    try:
        items = await fetch_ohgo_incidents(page_size=page_size, page_all=page_all, region=region,
                                           bounds_sw=bounds_sw, bounds_ne=bounds_ne, radius=radius)
        result = ingest_ohgo_incidents(db, items, return_detail=detail)
        if isinstance(result, dict):
            logger.info("[SYNC] manual_success source=OHGO inserted=%d updated=%d skipped=%d duration_sec=%.3f",
                        result.get("inserted", 0), result.get("updated", 0), result.get("skipped", 0),
                        (datetime.utcnow() - start).total_seconds())
        else:
            logger.info("[SYNC] manual_success source=OHGO processed=%d duration_sec=%.3f",
                        int(result), (datetime.utcnow() - start).total_seconds())
        return {"ok": True,
                "filters": {"page_all": page_all, "region": region, "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius},
                "result": (result if isinstance(result, dict) else {"processed": int(result)})}
    except Exception as e:
        logger.exception("[SYNC] manual_error source=OHGO err=%s", e)
        raise

@app.post("/ingest/ohio/roads")
async def ingest_ohgo_roads_endpoint(db: Session = Depends(get_db)):
    start = datetime.utcnow()
    logger.info("[SYNC] roads_start source=OHGO")
    items = await fetch_ohgo_roads()
    n = ingest_ohgo_roads(db, items)
    logger.info("[SYNC] roads_success source=OHGO count=%d duration_sec=%.3f", n, (datetime.utcnow() - start).total_seconds())
    return {"ok": True, "ingested": n}

@app.post("/ingest/texas/fetch")
async def ingest_texas_fetch(
    detail: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    items = await fetch_texas_incidents()
    result = ingest_texas_incidents(db, items, return_detail=detail)
    return {"ok": True, "result": result, "count": len(items)}

@app.get("/collect/ohio")
async def collect_ohio(
    detail: bool = False,
    include: Optional[List[str]] = Query(None),
    region: Optional[str] = None,
    bounds_sw: Optional[str] = None,
    bounds_ne: Optional[str] = None,
    radius: Optional[str] = None,
):
    start = datetime.utcnow()
    logger.info("[COLLECT] start include=%s params=%s", include, {"region": region, "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius})
    data = await fetch_ohgo_all(page_all=True, region=region, bounds_sw=bounds_sw, bounds_ne=bounds_ne, radius=radius, include=include)
    counts = {k: (len(v) if isinstance(v, list) else ("FeatureCollection" if isinstance(v, dict) else "object")) for k, v in data.items()}
    logger.info("[COLLECT] success counts=%s duration_sec=%.3f", counts, (datetime.utcnow() - start).total_seconds())
    resp = {"ok": True, "filters": {"page_all": True, "region": region, "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius}, "counts": counts}
    if detail: resp["data"] = data
    return resp

# ---------- log viewer endpoints ----------
def _log_file_path() -> Path:
    # Must mirror logging_config defaults
    configured = os.getenv("USACCIDENTS_LOG_FILE")
    if configured:
        return Path(configured)
    project_root = Path(__file__).resolve().parents[1]
    log_dir = Path(os.getenv("USACCIDENTS_LOG_DIR", project_root / "logs"))
    return log_dir / "usaccidents_app.log"

@app.get("/logs/tail", response_class=PlainTextResponse)
async def logs_tail(lines: int = Query(500, ge=1, le=5000)):
    p = _log_file_path()
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log file not found")
    # Efficient tail (read from end in chunks)
    chunk = 8192
    with p.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        out = bytearray()
        read = 0
        line_count = 0
        while size > 0 and line_count <= lines:
            step = min(chunk, size)
            size -= step
            f.seek(size)
            buf = f.read(step)
            out[:0] = buf  # prepend
            line_count = out.count(b"\n")
        # Keep only the last `lines` lines
        parts = out.splitlines()
        tail = parts[-lines:]
        text = b"\n".join(tail).decode("utf-8", errors="replace")
    return text

@app.get("/logs/download")
async def logs_download():
    p = _log_file_path()
    if not p.exists():
        raise HTTPException(status_code=404, detail="Log file not found")
    return FileResponse(str(p), filename=p.name, media_type="text/plain")
