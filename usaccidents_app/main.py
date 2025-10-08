#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/main.py
# Purpose: FastAPI app with API, scheduler, and web UI mounting.
#
# Description of code and how it works:
# - Includes /web/incidents (HTML) and /api/incidents (JSON filters) via webui router.
# - Mounts /static for assets.
# - Scheduler runs OHGO ingest every minute by passing coroutine directly.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.7.0
# Last Modified: 2025-10-07 by Tim Canady
#
# Revision History:
# - 0.7.0 (2025-10-07): Mount web UI and /api/incidents endpoint; /static files served.
###################################################################
#
from fastapi import FastAPI, Depends, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import case, desc
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.staticfiles import StaticFiles
import os

from .database import get_db
from .models import Incident
from .schemas import IncidentOut
from .connectors.ohio import fetch_ohgo_incidents, ingest_ohio_incidents, fetch_ohgo_roads, ingest_ohgo_roads
from .webui import router as web_router

app = FastAPI(title="usaccidents_app")
scheduler: Optional[AsyncIOScheduler] = None

# Mount static
STATIC_DIR = os.getenv("USACCIDENTS_STATIC", "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Include web UI router
app.include_router(web_router)

@app.on_event("startup")
async def _startup():
    global scheduler
    scheduler = AsyncIOScheduler()

    async def _scheduled_ohio_ingest():
        from .database import SessionLocal
        try:
            items = await fetch_ohgo_incidents(page_size=100)
            db = SessionLocal()
            try:
                ingest_ohio_incidents(db, items)
            finally:
                db.close()
        except Exception as e:
            print(f"[Scheduler] OHGO ingest error: {e}")

    scheduler.add_job(_scheduled_ohio_ingest, "interval", minutes=1)
    scheduler.start()

@app.on_event("shutdown")
async def _shutdown():
    if scheduler:
        scheduler.shutdown(wait=False)

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# MySQL-safe ordering: emulate NULLS LAST by ordering on IS NULL first
def _mysql_nulls_last_desc(col):
    return (case((col.is_(None), 1), else_=0), desc(col))

@app.get("/incidents/latest", response_model=List[IncidentOut])
async def incidents_latest(limit: int = Query(25, ge=1, le=200), db: Session = Depends(get_db)):
    q = (
        db.query(Incident)
        .order_by(*_mysql_nulls_last_desc(Incident.reported_time))
        .limit(limit)
    )
    return q.all()

@app.get("/incidents/changed_since", response_model=List[IncidentOut])
async def incidents_changed_since(
    since: Optional[str] = Query(None, description="ISO timestamp of last update"),
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
async def ingest_ohio_fetch(db: Session = Depends(get_db), page_size: int = 100):
    items = await fetch_ohgo_incidents(page_size=page_size)
    n = ingest_ohio_incidents(db, items)
    return {"ingested": n}

@app.post("/ingest/ohio/roads")
async def ingest_ohio_roads(db: Session = Depends(get_db)):
    items = await fetch_ohgo_roads()
    n = ingest_ohgo_roads(db, items)
    return {"ingested": n}
