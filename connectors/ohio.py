#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/ohio.py
# Purpose: OHGO connector and ingest helpers (robust URL building)
#
# Description of code and how it works:
# - Builds URLs safely from base + path without duplicating segments.
# - Tries Authorization header first, falls back to query param.
# - On 404 with a known duplicate-pattern (e.g., /incidents/incidents), retries once.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.6.1
# Last Modified: 2025-10-06 by Tim Canady
#
# Revision History:
# - 0.6.1 (2025-10-06): Robust _build_url; 404 dedup retry; light debug.
# - 0.6.0 (2025-10-04): Header→query auth fallback.
# - 0.5.0 (2025-09-28): Initial connector.
###################################################################
#
from typing import Dict, List, Optional
import os
import httpx
from datetime import datetime
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from ..models import Incident, Road

load_dotenv()

OHGO_API_KEY = os.getenv("OHGO_API_KEY")
OHGO_BASE_URL = os.getenv("OHGO_BASE_URL", "https://publicapi.ohgo.com/api/v1")
OHGO_INCIDENTS_PATH = os.getenv("OHGO_INCIDENTS_PATH", "/incidents")
OHGO_ROADS_PATH = os.getenv("OHGO_ROADS_PATH", "/roads")
APP_ENV = os.getenv("APP_ENV", "local").lower()


def _build_url(base: str, path: Optional[str]) -> str:
    base = (base or "").strip()
    if not base:
        raise ValueError("OHGO_BASE_URL is required")
    if not base.startswith(("http://", "https://")):
        base = "https://" + base.lstrip("/")
    base = base.rstrip("/")

    seg = (path or "").strip()
    if seg == "":
        return base
    seg = "/" + seg.lstrip("/")

    # If base already ends with seg, don't duplicate
    if base.lower().endswith(seg.lower()):
        return base

    # If last segment equals seg, skip append
    base_last = "/" + base.rsplit("/", 1)[-1].lower()
    if base_last == seg.lower():
        return base

    return base + seg


async def _get_json(url: str, params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None):
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params or {}, headers=headers or {})
        if resp.status_code in (401, 403) and OHGO_API_KEY:
            qp = dict(params or {})
            qp["api-key"] = OHGO_API_KEY
            resp = await client.get(url, params=qp)
        # If 404 and a common duplication is present, retry once without duplication
        if resp.status_code == 404:
            url_str = str(resp.request.url)
            if "/incidents/incidents" in url_str:
                rebound = url_str.replace("/incidents/incidents", "/incidents")
                resp = await client.get(rebound, params=params or {}, headers=headers or {})
        resp.raise_for_status()
        return resp.json()


def _auth() -> Dict[str, Dict[str, str]]:
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if OHGO_API_KEY:
        headers["Authorization"] = f"APIKEY {OHGO_API_KEY}"
        params["api-key"] = OHGO_API_KEY
    return {"headers": headers, "params": params}


async def fetch_ohgo_incidents(page_size: int = 100) -> List[Dict]:
    url = _build_url(OHGO_BASE_URL, OHGO_INCIDENTS_PATH)
    auth = _auth()
    data = await _get_json(url, params={"pageSize": page_size}, headers=auth["headers"])
    items = data.get("items", data)
    if APP_ENV in ("local", "dev", "debug"):
        print(f"[OHGO] fetched {len(items)} incidents from {url}")
    return items


def ingest_ohio_incidents(db: Session, items: List[Dict]) -> int:
    count = 0
    for item in items:
        uuid = item.get("uuid") or f"ohgo:{item.get('id') or item.get('eventId')}"
        source_event_id = str(item.get("id") or item.get("eventId") or "")
        state = item.get("state") or "OH"
        route = item.get("route")
        route_class = item.get("routeClass")

        existing = db.query(Incident).filter(Incident.uuid == uuid).one_or_none()
        if existing:
            existing.source_system = "OHGO"
            existing.source_event_id = source_event_id or existing.source_event_id
            existing.state = state
            existing.route = route
            existing.route_class = route_class
            existing.direction = item.get("direction")
            existing.latitude = item.get("latitude")
            existing.longitude = item.get("longitude")
            existing.reported_time = _parse_dt(item.get("reportedTime"))
            existing.updated_time = _parse_dt(item.get("updatedTime"))
            existing.cleared_time = _parse_dt(item.get("clearedTime"))
            existing.is_active = item.get("isActive")
            existing.event_type = item.get("eventType")
            existing.lanes_affected = item.get("lanesAffected")
            existing.closure_status = item.get("closureStatus")
            existing.severity_flag = item.get("severityFlag")
            existing.severity_score = item.get("severityScore")
            existing.raw_blob = item
        else:
            db.add(Incident(
                uuid=uuid,
                source_system="OHGO",
                source_event_id=source_event_id,
                source_url=item.get("url"),
                state=state,
                route=route,
                route_class=route_class,
                direction=item.get("direction"),
                latitude=item.get("latitude"),
                longitude=item.get("longitude"),
                reported_time=_parse_dt(item.get("reportedTime")),
                updated_time=_parse_dt(item.get("updatedTime")),
                cleared_time=_parse_dt(item.get("clearedTime")),
                is_active=item.get("isActive"),
                event_type=item.get("eventType"),
                lanes_affected=item.get("lanesAffected"),
                closure_status=item.get("closureStatus"),
                severity_flag=item.get("severityFlag"),
                severity_score=item.get("severityScore"),
                raw_blob=item,
            ))
        count += 1
    db.commit()
    return count


async def fetch_ohgo_roads() -> List[Dict]:
    url = _build_url(OHGO_BASE_URL, OHGO_ROADS_PATH)
    auth = _auth()
    data = await _get_json(url, headers=auth["headers"])
    items = data.get("items", data)
    if APP_ENV in ("local", "dev", "debug"):
        print(f"[OHGO] fetched {len(items)} roads from {url}")
    return items


def ingest_ohgo_roads(db: Session, items: List[Dict]) -> int:
    count = 0
    for item in items:
        source_system = "OHGO"
        road_id = str(item.get("id") or item.get("roadId"))
        existing = db.query(Road).filter(
            Road.source_system == source_system, Road.road_id == road_id
        ).one_or_none()
        common = dict(
            name=item.get("name"),
            description=item.get("description"),
            direction=item.get("direction"),
            begin_mile=item.get("beginMile"),
            end_mile=item.get("endMile"),
            length=item.get("length"),
            geometry=item.get("geometry"),
            last_updated=_parse_dt(item.get("lastUpdated")),
        )
        if existing:
            for k, v in common.items():
                setattr(existing, k, v)
        else:
            db.add(Road(source_system=source_system, road_id=road_id, **common))
        count += 1
    db.commit()
    return count


def _parse_dt(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
