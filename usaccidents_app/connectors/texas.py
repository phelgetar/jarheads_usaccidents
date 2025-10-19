#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/texas.py
# Purpose: DriveTexas connector (GeoJSON -> Incident upserts)
#
# Description of code and how it works:
# - Calls DriveTexas GeoJSON API with API key from env.
# - Normalizes features -> flat incident dicts compatible with models.Incident.
# - Defensive parsing for property name variants seen in DriveTexas feeds.
# - Idempotent upserts keyed by (source_system, source_event_id) with uuid fallback.
#
# Author: Tim Canady
# Created: 2025-10-14
#
# Version: 0.1.0
# Last Modified: 2025-10-14 by Tim Canady
#
# Revision History:
# - 0.1.0 (2025-10-14): Initial DriveTexas fetch/ingest implementation.
###################################################################
#
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import os
import json
from datetime import datetime
import httpx
from sqlalchemy.orm import Session

from ..models import Incident

# ----------------------- ENV / CONFIG ------------------------

DRIVETEXAS_API_KEY = os.getenv("DRIVETEXAS_API_KEY") or os.getenv("DRIVE_TEXAS_API_KEY")
DRIVETEXAS_BASE_URL = os.getenv("DRIVETEXAS_BASE_URL", "https://api.drivetexas.org/api/conditions.geojson")
APP_ENV = (os.getenv("APP_ENV") or "local").lower()


# ----------------------- HELPERS -----------------------------

def _iso_to_dt(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        try:
            # tolerate fractional seconds without Z
            return datetime.fromisoformat(value)
        except Exception:
            return None

def _clip(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _get(d: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def _compute_active(cleared_time: Optional[datetime]) -> Optional[bool]:
    if cleared_time is None:
        return True
    return cleared_time > datetime.utcnow()

def _severity_from_flags(props: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    """
    Rough mapping from DriveTexas flags to severity.
    """
    desc = str(_get(props, "description", "DESCRIPTION", default="") or "").lower()
    delay_flag = str(_get(props, "delay_flag", "DELAY_FLAG", default="") or "").lower()

    flag: Optional[str] = None
    score: Optional[int] = None

    if "closed" in desc:
        flag, score = "HIGH", 3
    elif delay_flag in ("true", "1", "yes"):
        flag, score = "MEDIUM", 2
    elif "lane blocked" in desc or "shoulder blocked" in desc:
        flag, score = "MEDIUM", 2
    elif desc:
        flag, score = "LOW", 1

    return flag, score

def _closure_status_from_desc(props: Dict[str, Any]) -> Optional[str]:
    desc = str(_get(props, "description", "DESCRIPTION", default="") or "").lower()
    if "closed" in desc:
        return "CLOSED"
    if "lane blocked" in desc or "shoulder blocked" in desc:
        return "PARTIAL"
    if "open" in desc:
        return "OPEN"
    return None

def _normalize_feature(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    DriveTexas GeoJSON feature -> normalized dict for ingestion.
    Observed properties:
      GLOBALID / Identifier (id), route_name, travel_direction, from_ref_marker,
      start_time, end_time, create_time, description, condition, delay_flag, county_num
    Geometry: Point -> [lon, lat]
    """
    if not isinstance(feature, dict):
        return None

    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or []
    if not isinstance(coords, list) or len(coords) < 2:
        return None

    lon, lat = coords[0], coords[1]

    source_id = _get(props, "GLOBALID", "Identifier", "id")
    if not source_id:
        return None

    route = _get(props, "route_name", "ROUTE_NAME")
    direction = _get(props, "travel_direction", "DIRECTION")

    reported = _iso_to_dt(_get(props, "start_time", "START_TIME"))
    updated = _iso_to_dt(_get(props, "create_time", "CREATE_TIME", "start_time", "START_TIME"))
    cleared = _iso_to_dt(_get(props, "end_time", "END_TIME"))
    is_active = _compute_active(cleared)

    event_type = _get(props, "condition", "CONDITION", default="Unknown")
    milepost_raw = _get(props, "from_ref_marker", "FROM_REF_MARKER")
    try:
        milepost = float(milepost_raw) if milepost_raw is not None else None
    except Exception:
        milepost = None

    closure_status = _closure_status_from_desc(props)
    severity_flag, severity_score = _severity_from_flags(props)

    obj = {
        "uuid": f"drivetexas:{source_id}",
        "id": str(source_id),
        "source_system": "DRIVETEXAS",
        "source_event_id": str(source_id),
        "state": "TX",
        "county": _get(props, "county_num", "COUNTY_NUM"),
        "route": route,
        "routeClass": None,
        "direction": _clip(direction, 32),
        "milepost": milepost,
        "latitude": float(lat) if lat is not None else None,
        "longitude": float(lon) if lon is not None else None,
        "reportedTime": reported.isoformat() if reported else None,
        "updatedTime": updated.isoformat() if updated else None,
        "clearedTime": cleared.isoformat() if cleared else None,
        "isActive": is_active,
        "eventType": event_type,
        "lanesAffected": _get(props, "description", "DESCRIPTION"),
        "closureStatus": closure_status,
        "severityFlag": severity_flag,
        "severityScore": severity_score,
        "url": None,
        "_raw": {"properties": props, "geometry": geom},
    }
    return obj


# ----------------------- FETCH -------------------------------

async def fetch_texas_incidents() -> List[Dict[str, Any]]:
    """
    Call DriveTexas GeoJSON and return normalized list-of-dicts.
    """
    if not DRIVETEXAS_API_KEY:
        if APP_ENV in ("local", "dev", "debug"):
            print("[TEXAS] missing DRIVETEXAS_API_KEY in env")
        return []

    params = {"key": DRIVETEXAS_API_KEY}
    url = DRIVETEXAS_BASE_URL

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params, headers={"Accept": "application/json", "User-Agent": "USAccidents/DriveTexas"})
        if resp.status_code != 200:
            resp.raise_for_status()
        try:
            data = resp.json()
        except Exception:
            data = json.loads(resp.text)

    feats = data.get("features", []) if isinstance(data, dict) else []
    items: List[Dict[str, Any]] = []
    for f in feats:
        norm = _normalize_feature(f)
        if norm:
            items.append(norm)

    if APP_ENV in ("local", "dev", "debug"):
        print(f"[TEXAS] fetched {len(items)} incidents from {url}")
    return items


# ----------------------- INGEST ------------------------------

def ingest_texas_incidents(db: Session, items: List[Dict[str, Any]], return_detail: bool = True):
    """
    Upsert into incidents table. Uses (source_system, source_event_id) and uuid fallback.
    Returns inserted/updated/skipped.
    """
    inserted = updated = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        uuid = item.get("uuid") or f"drivetexas:{item.get('id')}"
        source_event_id = str(item.get("id") or item.get("source_event_id") or "")
        if not source_event_id:
            skipped += 1
            continue

        existing = (
            db.query(Incident)
              .filter(Incident.source_system == "DRIVETEXAS", Incident.source_event_id == source_event_id)
              .one_or_none()
        )
        if not existing and uuid:
            existing = db.query(Incident).filter(Incident.uuid == uuid).one_or_none()

        common = dict(
            source_system="DRIVETEXAS",
            source_event_id=source_event_id,
            state=item.get("state") or "TX",
            county=item.get("county"),
            route=item.get("route"),
            route_class=item.get("routeClass"),
            direction=_clip(item.get("direction"), 32),
            milepost=item.get("milepost"),
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            reported_time=_iso_to_dt(item.get("reportedTime")),
            updated_time=_iso_to_dt(item.get("updatedTime")),
            cleared_time=_iso_to_dt(item.get("clearedTime")),
            is_active=item.get("isActive"),
            event_type=item.get("eventType"),
            lanes_affected=item.get("lanesAffected"),
            closure_status=item.get("closureStatus"),
            severity_flag=item.get("severityFlag"),
            severity_score=item.get("severityScore"),
        )

        if existing:
            for k, v in common.items():
                setattr(existing, k, v)
            existing.raw_blob = item.get("_raw") or item
            updated += 1
        else:
            db.add(Incident(
                uuid=uuid,
                source_system=common["source_system"],
                source_event_id=common["source_event_id"],
                source_url=item.get("url"),
                state=common["state"],
                county=common["county"],
                route=common["route"],
                route_class=common["route_class"],
                direction=common["direction"],
                milepost=common["milepost"],
                latitude=common["latitude"],
                longitude=common["longitude"],
                reported_time=common["reported_time"],
                updated_time=common["updated_time"],
                cleared_time=common["cleared_time"],
                is_active=common["is_active"],
                event_type=common["event_type"],
                lanes_affected=common["lanes_affected"],
                closure_status=common["closure_status"],
                severity_flag=common["severity_flag"],
                severity_score=common["severity_score"],
                raw_blob=item.get("_raw") or item,
            ))
            inserted += 1

    db.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped} if return_detail else (inserted + updated)
