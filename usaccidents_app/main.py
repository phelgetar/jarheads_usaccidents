#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/main.py
# Purpose: FastAPI app (incidents API, OHGO ingest, and collector)
#
# Description of code and how it works:
# - Scheduler: AsyncIOScheduler calls ingest coroutine directly every minute.
# - MySQL-safe ordering helpers for latest/changed_since.
# - Endpoints to ingest incidents/roads and to collect all OHGO resources.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.9.1
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.9.1 (2025-10-08): Switch to ingest_ohgo_* names.
# - 0.9.0 (2025-10-08): Add /collect/ohio with resilient collectors; minor refactors.
# - 0.8.0 (2025-10-08): Expose OHGO filters on ingest endpoint; default page_all=true.
# - 0.7.0 (2025-10-08): Add /ingest detail flag; robust startup scheduler.
###################################################################
#
#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/main.py
# Purpose: FastAPI app (incidents API, OHGO ingest, collector, active-count)
#
# Description of code and how it works:
# - Scheduler: AsyncIOScheduler calls ingest coroutine directly every minute.
# - MySQL-safe ordering helpers for latest/changed_since.
# - Endpoints: ingest incidents/roads, collect OHGO, and live active_count.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.9.3
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.9.3 (2025-10-08): Add /incidents/active_count and surface in web UI.
# - 0.9.1 (2025-10-08): Switch to ingest_ohgo_* names.
# - 0.9.0 (2025-10-08): Add /collect/ohio with resilient collectors.
###################################################################
#
from typing import List, Optional
from fastapi import FastAPI, Depends, Query
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import case, desc, or_, and_
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pathlib import Path
import os

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
        try:
            items = await fetch_ohgo_incidents(page_all=True)
            db = SessionLocal()
            try:
                ingest_ohgo_incidents(db, items, return_detail=False)
            finally:
                db.close()
        except Exception as e:
            print(f"[Scheduler] OHGO ingest error: {e}")

    scheduler.add_job(_scheduled_ohio_ingest, "interval", minutes=1)  # direct coro
    scheduler.start()

@app.on_event("shutdown")
async def _shutdown():
    if scheduler:
        scheduler.shutdown(wait=False)

def _mysql_nulls_last_desc(col):
    return (case((col.is_(None), 1), else_=0), desc(col))

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/incidents/active_count")
async def incidents_active_count(db: Session = Depends(get_db)):
    # Count active rows. Treat NULL is_active + NULL cleared_time as active (backfill safety).
    cnt = (
        db.query(Incident)
          .filter(
              or_(
                  Incident.is_active.is_(True),
                  and_(Incident.is_active.is_(None), Incident.cleared_time.is_(None)),
              )
          )
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
        from datetime import datetime
        try:
            dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            q = q.filter(Incident.updated_time >= dt)
        except Exception:
            pass
    q = q.order_by(*_mysql_nulls_last_desc(Incident.updated_time)).limit(limit)
    return q.all()

@app.post("/ingest/ohio/fetch")
async def ingest_ohgo_fetch(
    db: Session = Depends(get_db),
    page_size: int = Query(500, ge=1, le=2000, description="Ignored when page_all=true"),
    page_all: bool = Query(True, description="Use OHGO page-all to fetch complete set"),
    region: Optional[str] = Query(None, description="e.g., 'columbus,cleveland' or 'ne-ohio'"),
    bounds_sw: Optional[str] = Query(None, description="lat,lon (south-west corner)"),
    bounds_ne: Optional[str] = Query(None, description="lat,lon (north-east corner)"),
    radius: Optional[str] = Query(None, description="lat,lon,miles"),
    detail: bool = Query(True, description="Return inserted/updated/skipped when true"),
):
    items = await fetch_ohgo_incidents(
        page_size=page_size,
        page_all=page_all,
        region=region,
        bounds_sw=bounds_sw,
        bounds_ne=bounds_ne,
        radius=radius,
    )
    result = ingest_ohgo_incidents(db, items, return_detail=detail)
    return {
        "ok": True,
        "filters": {"page_all": page_all, "region": region, "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius},
        "result": (result if isinstance(result, dict) else {"processed": int(result)}),
    }

@app.post("/ingest/ohio/roads")
async def ingest_ohgo_roads_endpoint(db: Session = Depends(get_db)):
    items = await fetch_ohgo_roads()
    n = ingest_ohgo_roads(db, items)
    return {"ok": True, "ingested": n}

@app.get("/collect/ohio")
async def collect_ohio(
    detail: bool = False,
    include: Optional[List[str]] = Query(
        None,
        description=("Subset of: incidents,construction,digital_signs,cameras,travel_delays,"
                     "dangerous_slowdowns,truck_parking,weather_sensor_sites,work_zones_wzdx")
    ),
    region: Optional[str] = None,
    bounds_sw: Optional[str] = None,
    bounds_ne: Optional[str] = None,
    radius: Optional[str] = None,
):
    data = await fetch_ohgo_all(
        page_all=True,
        region=region,
        bounds_sw=bounds_sw,
        bounds_ne=bounds_ne,
        radius=radius,
        include=include,
    )
    counts = {k: (len(v) if isinstance(v, list) else ("FeatureCollection" if isinstance(v, dict) else "object"))
              for k, v in data.items()}
    resp = {
        "ok": True,
        "filters": {"page_all": True, "region": region, "bounds_sw": bounds_sw, "bounds_ne": bounds_ne, "radius": radius},
        "counts": counts,
    }
    if detail:
        resp["data"] = data
    return resp
