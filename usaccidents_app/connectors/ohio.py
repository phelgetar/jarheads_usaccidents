#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/ohio.py
# Purpose: OHGO connector (robust JSON/type handling)
#
# Description of code and how it works:
# - _get_json always returns parsed JSON (dict or list), or [] on failure.
# - _as_items normalizes payload into a list of dicts.
# - ingest_ohio_* skip any non-dict items safely.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.1.0
# Last Modified: 2025-10-06 by Tim Canady
#
# Revision History:
# - 0.1.0 (2025-10-06): Initial release of header patcher.
###################################################################
#
from typing import Dict, List, Optional, Any
import os
import json
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
    if not base.startswith(("http://", "https://")):
        base = "https://" + base.lstrip("/")
    base = base.rstrip("/")
    seg = (path or "").strip()
    if seg:
        seg = "/" + seg.lstrip("/")
        if not base.lower().endswith(seg.lower()) and ("/" + base.rsplit("/", 1)[-1].lower()) != seg.lower():
            base = base + seg
    return base


async def _get_json(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params or {}, headers=headers or {})
        if resp.status_code in (401, 403) and OHGO_API_KEY:
            qp = dict(params or {})
            qp["api-key"] = OHGO_API_KEY
            resp = await client.get(url, params=qp)

        # Common duplication retry
        if resp.status_code == 404 and "/incidents/incidents" in str(resp.request.url):
            rebound = str(resp.request.url).replace("/incidents/incidents", "/incidents")
            resp = await client.get(rebound, params=params or {}, headers=headers or {})

        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            # Try manual json loading from text
            try:
                return json.loads(resp.text)
            except Exception:
                if APP_ENV in ("local", "dev", "debug"):
                    print(f"[OHGO] Non-JSON response from {url[:120]}... -> returning []")
                return []


def _as_items(data: Any) -> List[Dict[str, Any]]:
    """
    Normalize various possible payload shapes into a list of dicts.
    """
    if isinstance(data, dict):
        candidate = data.get("items", data.get("data", data.get("results", data)))
        if isinstance(candidate, list):
            return [x for x in candidate if isinstance(x, dict)]
        elif isinstance(candidate, dict):
            return [candidate]
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
            return _as_items(parsed)
        except Exception:
            return []
    return []


def _auth() -> Dict[str, Dict[str, str]]:
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if OHGO_API_KEY:
        headers["Authorization"] = f"APIKEY {OHGO_API_KEY}"
        params["api-key"] = OHGO_API_KEY
    return {"headers": headers, "params": params}


async def fetch_ohgo_incidents(page_size: int = 100) -> List[Dict[str, Any]]:
    url = _build_url(OHGO_BASE_URL, OHGO_INCIDENTS_PATH)
    auth = _auth()
    data = await _get_json(url, params={"pageSize": page_size}, headers=auth["headers"])
    items = _as_items(data)
    if APP_ENV in ("local", "dev", "debug"):
        print(f"[OHGO] fetched {len(items)} incidents from {url}")
    return items


def ingest_ohio_incidents(db: Session, items: List[Dict[str, Any]]) -> int:
    count = 0
    for item in items:
        if not isinstance(item, dict):
            continue  # skip bad shapes defensively
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


async def fetch_ohgo_roads() -> List[Dict[str, Any]]:
    url = _build_url(OHGO_BASE_URL, OHGO_ROADS_PATH)
    auth = _auth()
    data = await _get_json(url, headers=auth["headers"])
    items = _as_items(data)
    if APP_ENV in ("local", "dev", "debug"):
        print(f"[OHGO] fetched {len(items)} roads from {url}")
    return items


def ingest_ohgo_roads(db: Session, items: List[Dict[str, Any]]) -> int:
    count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        source_system = "OHGO"
        road_id = str(item.get("id") or item.get("roadId"))
        existing = db.query(Road).filter(Road.source_system == source_system, Road.road_id == road_id).one_or_none()
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
