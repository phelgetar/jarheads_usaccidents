#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/ohio.py
# Purpose: OHGO connector (full fetch, duplicate-safe ingest, collectors)
#
# Description of code and how it works:
# - Logs all fetches and ingest outcomes to the rotating log file.
# - Per-item commit with IntegrityError fallback (race-safe upsert).
# - Upserts: per-item commit + IntegrityError fallback to update (race-safe).
# - Uses OHGO documented filters; default is **page-all=true** to fetch the full statewide set.
# - Optional filters via env: OHGO_REGION, OHGO_BOUNDS_SW, OHGO_BOUNDS_NE, OHGO_RADIUS "lat,lon,miles".
# - API Result wrapper parsed **case-insensitively** (handles Results/links/TotalResultCount).
# - Falls back across param styles: kebab-case → camelCase for page-all/page-size/page.
# - Collects link[rel=self] as source_url; maps OHGO “Category/RouteName/RoadStatus” to our fields.
# - Ingest returns detailed counts when requested (inserted/updated/skipped).
# - Incidents: supports page-all + region/bounds/radius filters; tolerant JSON parsing.
# - Ingest: idempotent updates by checking uuid candidates + (source_system, source_event_id).
# - Derivations: fills is_active and severity_score when OHGO omits them.
# - API collectors: fetch cameras, construction, etc., using API-root normalization; skip 404s.
# - Structured logging compatible with logging_config.py.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.10.0
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.10.0 (2025-10-09): Add fetch_ohgo_all + generic collectors with base-path fix.
# - 0.9.4 (2025-10-09): Replace prints with structured logger; add fetch/ingest diagnostics.
# - 0.9.3 (2025-10-08): Upserts with per-item commit + IntegrityError fallback.
# - 0.9.2 (2025-10-08): Derive is_active & severity_score when absent in payloads.
# - 0.9.1 (2025-10-08): Rename ingest functions to ingest_ohgo_*; keep compat aliases.
# - 0.9.0 (2025-10-08): Collector hardening (API-root normalize, 404 skip) + /collect support.
# - 0.8.1 (2025-10-08): Duplicate-safe ingest; standardize new uuid to 'ohgo:<id>'.
# - 0.8.0 (2025-10-08): Full-fetch + filters + case-insensitive parsing.
# - 0.6.3 (2025-10-07): Clip direction to 32 chars to match widened schema.
###################################################################
#
from __future__ import annotations
from typing import Dict, List, Optional, Any, Tuple
import os
import json
import httpx
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

from ..models import Incident, Road

load_dotenv()
log = logging.getLogger("usaccidents")

# --- Environment / defaults ---
OHGO_API_KEY = os.getenv("OHGO_API_KEY")
OHGO_BASE_URL = os.getenv("OHGO_BASE_URL", "https://publicapi.ohgo.com/api/v1")
OHGO_INCIDENTS_PATH = os.getenv("OHGO_INCIDENTS_PATH", "/incidents")
OHGO_ROADS_PATH = os.getenv("OHGO_ROADS_PATH", "/roads")
OHGO_REGION = os.getenv("OHGO_REGION")
OHGO_BOUNDS_SW = os.getenv("OHGO_BOUNDS_SW")
OHGO_BOUNDS_NE = os.getenv("OHGO_BOUNDS_NE")
OHGO_RADIUS = os.getenv("OHGO_RADIUS")
APP_ENV = (os.getenv("APP_ENV") or "local").lower()

# ---- URL & auth helpers -------------------------------------------------------

def _build_url(base: str, path: Optional[str]) -> str:
    base = (base or "").strip()
    if not base.startswith(("http://", "https://")):
        base = "https://" + base.lstrip("/")
    base = base.rstrip("/")
    seg = (path or "").strip()
    if seg:
        seg = "/" + seg.lstrip("/")
        # avoid double-appending if base already ends with seg
        if not base.lower().endswith(seg.lower()):
            base = base + seg
    return base

def _api_root(base: str) -> str:
    """
    If base ends with a resource (e.g., /incidents), return its parent (true API root).
    Prevents building /incidents/construction.
    """
    u = (base or "").rstrip("/")
    tail = u.rsplit("/", 1)[-1].lower()
    known = {
        "incidents","roads","construction","digital-signs","cameras",
        "travel-delays","truck-parking","weather-sensor-sites",
        "dangerous-slowdowns","work-zones"
    }
    return u.rsplit("/", 1)[0] if tail in known else u

def _auth() -> Dict[str, Dict[str, str]]:
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if OHGO_API_KEY:
        headers["Authorization"] = f"APIKEY {OHGO_API_KEY}"
        params["api-key"] = OHGO_API_KEY
    return {"headers": headers, "params": params}

# ---- HTTP / JSON helpers ------------------------------------------------------

async def _request_json(client: httpx.AsyncClient, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Any:
    resp = await client.get(url, params=params, headers=headers or {})
    # header -> query fallback
    if resp.status_code in (401, 403) and OHGO_API_KEY:
        qp = dict(params); qp["api-key"] = OHGO_API_KEY
        resp = await client.get(url, params=qp)
    # duplicate path guard
    if resp.status_code == 404 and "/incidents/incidents" in str(resp.request.url):
        rebound = str(resp.request.url).replace("/incidents/incidents", "/incidents")
        resp = await client.get(rebound, params=params, headers=headers or {})
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        try:
            return json.loads(resp.text)
        except Exception:
            return []

def _ci_get(d: Dict[str, Any], key: str, default=None):
    lk = key.lower()
    for k, v in d.items():
        if str(k).lower() == lk:
            return v
    return default

def _as_items(data: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    if isinstance(data, dict):
        for k in ("Results","items","data","results","incidents","content","value"):
            val = _ci_get(data, k)
            if isinstance(val, list):
                for mk in ("TotalResultCount","CurrentResultCount","TotalPageCount","LastUpdated"):
                    mv = _ci_get(data, mk)
                    if mv is not None: meta[mk] = mv
                return [x for x in val if isinstance(x, dict)], meta
            if isinstance(val, dict):
                return [val], meta
        feats = _ci_get(data, "features")
        if isinstance(feats, list):
            return [x for x in feats if isinstance(x, dict)], meta
        return ([data] if data else []), meta
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)], meta
    if isinstance(data, str):
        try:
            return _as_items(json.loads(data))
        except Exception:
            return [], meta
    return [], meta

# ---- small utils --------------------------------------------------------------

def _clip(val: Optional[str], maxlen: int) -> Optional[str]:
    if val is None: return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]

def _parse_dt(value: Optional[str]):
    if not value: return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None

def _extract_link(item: Dict[str, Any], rel: str) -> Optional[str]:
    links = item.get("links") or item.get("Links") or []
    if isinstance(links, list):
        for ln in links:
            if isinstance(ln, dict) and (ln.get("rel") or ln.get("Rel")) == rel:
                return ln.get("href") or ln.get("Href")
    return None

# ---- derived fields -----------------------------------------------------------

_SEVERITY_TEXT_MAP = {"severe":3,"major":3,"high":3,"moderate":2,"medium":2,"minor":1,"low":1}
_STATUS_SCORE_MAP = {"closed":3,"restricted":2,"incident":2,"delay":2,"active":2,"open":1,"cleared":1,"completed":1,"ended":1}
_CATEGORY_SCORE_MAP = {"crash":3,"accident":3,"work zone":2,"construction":2,"maintenance":1,"repairs/maintenance":1,"disabled vehicle":1,"hazard":2}

def _derive_is_active(is_active: Optional[bool], cleared_time: Optional[str], end_time: Optional[str], status: Optional[str]) -> Optional[bool]:
    if isinstance(is_active, bool): return is_active
    if cleared_time or end_time: return False
    if status:
        s = str(status).strip().lower()
        if s in {"cleared","completed","ended"}: return False
        if s in {"closed","restricted","incident","delay","active","open"}: return True
    return True

def _derive_severity(severity_score: Optional[Any], severity_flag: Optional[str], status: Optional[str], category: Optional[str]) -> (Optional[str], Optional[int]):
    if severity_score is not None:
        try: return severity_flag, int(severity_score)
        except Exception: pass
    if severity_flag:
        score = _SEVERITY_TEXT_MAP.get(str(severity_flag).strip().lower())
        if score: return severity_flag, score
    if status:
        score = _STATUS_SCORE_MAP.get(str(status).strip().lower())
        if score: return (severity_flag or status.title()), score
    if category:
        c = str(category).strip().lower()
        for key, score in _CATEGORY_SCORE_MAP.items():
            if key in c:
                return (severity_flag or category.title()), score
    return severity_flag, None

# ---- normalizers --------------------------------------------------------------

def _normalize_incident(item: Dict[str, Any]) -> Dict[str, Any]:
    # GeoJSON-like?
    props = item.get("properties"); geom = item.get("geometry")
    if isinstance(props, dict):
        coords = (geom or {}).get("coordinates") if isinstance(geom, dict) else None
        lat = coords[1] if (isinstance(coords, list) and len(coords) > 1) else None
        lon = coords[0] if (isinstance(coords, list) and len(coords) > 0) else None
        route_name = props.get("routeName")
        route_class = "INTERSTATE" if str(route_name or "").startswith("I-") else "STATE"
        raw_is_active = props.get("isActive")
        start_time = props.get("startTime"); last_updated = props.get("lastUpdated")
        end_time = props.get("endTime")
        status = props.get("status") or props.get("roadStatus")
        category = props.get("type") or props.get("category")
        sev_flag = props.get("severity"); sev_score = props.get("severityScore")
        is_active = _derive_is_active(raw_is_active, end_time, end_time, status)
        sev_flag, sev_score = _derive_severity(sev_score, sev_flag, status, category)
        return {
            "uuid": item.get("id"),
            "id": item.get("id"),
            "state": props.get("state", "OH"),
            "route": route_name,
            "routeClass": route_class,
            "direction": props.get("direction"),
            "latitude": lat,
            "longitude": lon,
            "reportedTime": start_time,
            "updatedTime": last_updated,
            "clearedTime": end_time,
            "isActive": is_active,
            "eventType": category,
            "lanesAffected": props.get("lanesAffected"),
            "closureStatus": status,
            "severityFlag": sev_flag,
            "severityScore": sev_score,
            "url": _extract_link(item, "self") or item.get("url"),
        }

    # Flat-ish
    status = item.get("roadStatus") or item.get("status")
    category = item.get("category") or item.get("type")
    end_time = item.get("clearedTime") or item.get("endTime")
    raw_is_active = item.get("isActive")
    sev_flag = item.get("severity"); sev_score = item.get("severityScore")
    is_active = _derive_is_active(raw_is_active, end_time, end_time, status)
    sev_flag, sev_score = _derive_severity(sev_score, sev_flag, status, category)
    return {
        "uuid": item.get("id"),
        "id": item.get("id"),
        "state": item.get("state") or "OH",
        "route": item.get("routeName"),
        "routeClass": ("INTERSTATE" if str(item.get("routeName") or "").startswith("I-") else "STATE"),
        "direction": item.get("direction"),
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "reportedTime": item.get("reportedTime") or item.get("startTime"),
        "updatedTime": item.get("updatedTime") or item.get("lastUpdated"),
        "clearedTime": end_time,
        "isActive": is_active,
        "eventType": category,
        "lanesAffected": item.get("lanesAffected"),
        "closureStatus": status,
        "severityFlag": sev_flag,
        "severityScore": sev_score,
        "url": _extract_link(item, "self") or item.get("url"),
        "location": item.get("location"),
        "description": item.get("description"),
        "roadClosureDetails": item.get("roadClosureDetails"),
    }

# ---- primary incidents fetch --------------------------------------------------

async def fetch_ohgo_incidents(page_size: int = 100, page_all: bool = True,
                               region: Optional[str] = None,
                               bounds_sw: Optional[str] = None,
                               bounds_ne: Optional[str] = None,
                               radius: Optional[str] = None) -> List[Dict[str, Any]]:
    url = _build_url(OHGO_BASE_URL, OHGO_INCIDENTS_PATH)
    auth = _auth()
    params: Dict[str, Any] = {}
    if page_all:
        params["page-all"] = "true"
    else:
        params["page-size"] = page_size
        params["page"] = 1

    if region or OHGO_REGION:
        params["region"] = region or OHGO_REGION
    if bounds_sw and bounds_ne:
        params["map-bounds-sw"] = bounds_sw
        params["map-bounds-ne"] = bounds_ne
    elif OHGO_BOUNDS_SW and OHGO_BOUNDS_NE:
        params["map-bounds-sw"] = OHGO_BOUNDS_SW
        params["map-bounds-ne"] = OHGO_BOUNDS_NE
    if radius or OHGO_RADIUS:
        params["radius"] = radius or OHGO_RADIUS

    log.info("[OHGO] fetch incidents start url=%s params=%s",
             url, {k: v for k, v in params.items() if k != "api-key"})

    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, params, auth["headers"])
        # try camelCase for some deployments
        if isinstance(data, dict) and ("Error" in data or "error" in data) and page_all:
            alt = dict(params); alt.pop("page-all", None); alt["pageAll"] = "true"
            data = await _request_json(client, url, alt, auth["headers"])

        items, meta = _as_items(data)

        # Manual pagination if not page-all and meta says there are more pages
        if not page_all and isinstance(data, dict):
            all_items = list(items)
            page_num = int(params.get("page", 1)) if str(params.get("page", 1)).isdigit() else 1
            while True:
                total_pages = int(data.get("TotalPageCount") or data.get("totalPages") or 0) if isinstance(data, dict) else 0
                page_num += 1
                if total_pages and page_num > total_pages:
                    break
                if len(items) == 0:
                    break
                nxt = dict(params); nxt["page"] = page_num
                data = await _request_json(client, url, nxt, auth["headers"])
                items, _ = _as_items(data)
                if not items:
                    break
                all_items.extend(items)
            items = all_items

    out: List[Dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        norm = _normalize_incident(raw)
        norm["direction"] = _clip(norm.get("direction"), 32)
        out.append(norm)

    log.info("[OHGO] fetch incidents done count=%d", len(out))
    return out

# ---- ingest incidents (duplicate-safe) ---------------------------------------

def _uuid_candidates(source_event_id: str, item_uuid: Optional[str]) -> List[str]:
    c = []
    if item_uuid:
        c.append(str(item_uuid))
    if source_event_id:
        c.append(str(source_event_id))
        c.append(f"ohgo:{source_event_id}")
    return list(dict.fromkeys(c))

def _apply_common(existing: Incident, common: Dict[str, Any], item: Dict[str, Any]):
    for k, v in common.items():
        setattr(existing, k, v)
    existing.source_url = item.get("url")
    existing.raw_blob = item

def ingest_ohgo_incidents(db: Session, items: List[Dict[str, Any]], return_detail: bool = False):
    inserted, updated, skipped = 0, 0, 0
    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        source_event_id = str(item.get("id") or item.get("eventId") or item.get("uuid") or "")
        if not source_event_id:
            skipped += 1
            continue

        candidates = _uuid_candidates(source_event_id, item.get("uuid"))
        existing = (db.query(Incident)
                      .filter(or_(Incident.uuid.in_(candidates),
                                  and_(Incident.source_system == "OHGO", Incident.source_event_id == source_event_id)))
                      .first())

        common = dict(
            source_system="OHGO",
            source_event_id=source_event_id,
            state=item.get("state") or "OH",
            route=item.get("route"),
            route_class=item.get("routeClass"),
            direction=_clip(item.get("direction"), 32),
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
        )

        if existing:
            _apply_common(existing, common, item)
            db.commit()
            updated += 1
        else:
            new_uuid = f"ohgo:{source_event_id}"
            row = Incident(
                uuid=new_uuid,
                source_system=common["source_system"],
                source_event_id=common["source_event_id"],
                source_url=item.get("url"),
                state=common["state"],
                route=common["route"],
                route_class=common["route_class"],
                direction=common["direction"],
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
                raw_blob=item,
            )
            db.add(row)
            try:
                db.commit()
                inserted += 1
            except IntegrityError:
                db.rollback()
                existing2 = (db.query(Incident)
                               .filter(or_(Incident.uuid.in_(candidates + [new_uuid]),
                                           and_(Incident.source_system == "OHGO", Incident.source_event_id == source_event_id)))
                               .first())
                if existing2:
                    _apply_common(existing2, common, item)
                    db.commit()
                    updated += 1
                else:
                    skipped += 1

    if return_detail:
        log.info("[OHGO] ingest result inserted=%d updated=%d skipped=%d", inserted, updated, skipped)
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    else:
        log.info("[OHGO] ingest result processed=%d", inserted + updated)
        return inserted + updated

# Back-compat alias (main app prefers ingest_ohgo_incidents)
ingest_ohio_incidents = ingest_ohgo_incidents

# ---- roads -------------------------------------------------------------------

async def fetch_ohgo_roads() -> List[Dict[str, Any]]:
    url = _build_url(OHGO_BASE_URL, OHGO_ROADS_PATH)
    auth = _auth()
    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, {}, auth["headers"])
    items, _ = _as_items(data)
    log.info("[OHGO] fetch roads count=%d", len(items))
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
            direction=_clip(item.get("direction"), 20),
            begin_mile=item.get("beginMile") or item.get("beginMilepost"),
            end_mile=item.get("endMile") or item.get("endMilepost"),
            length=item.get("length"),
            geometry=item.get("geometry"),
            last_updated=_parse_dt(item.get("lastUpdated")),
        )
        if existing:
            for k, v in common.items():
                setattr(existing, k, v)
        else:
            db.add(Road(source_system=source_system, road_id=road_id, **common))
        db.commit()
        count += 1
    log.info("[OHGO] ingest roads updated_or_inserted=%d", count)
    return count

# Back-compat alias
ingest_ohio_roads = ingest_ohgo_roads

# ---- general collectors (for /collect/ohio) ----------------------------------

async def _fetch_items(endpoint: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generic fetch for non-incident collections (construction, cameras, etc.)."""
    root = _api_root(OHGO_BASE_URL)
    url = _build_url(root, endpoint)
    auth = _auth()
    q = {}

    # pass-through supported filters
    for k in ("region", "map-bounds-sw", "map-bounds-ne", "radius", "page-all", "pageAll"):
        if k in filters and filters[k] is not None:
            q[k] = filters[k]

    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, q, auth["headers"])
    items, _ = _as_items(data)
    return items

async def fetch_ohgo_construction(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/construction", filters or {})

async def fetch_ohgo_digital_signs(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/digital-signs", filters or {})

async def fetch_ohgo_cameras(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/cameras", filters or {})

async def fetch_ohgo_travel_delays(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/travel-delays", filters or {})

async def fetch_ohgo_dangerous_slowdowns(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/dangerous-slowdowns", filters or {})

async def fetch_ohgo_truck_parking(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/truck-parking", filters or {})

async def fetch_ohgo_weather_sensor_sites(filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return await _fetch_items("/weather-sensor-sites", filters or {})

async def fetch_ohgo_work_zones_wzdx(filters: Optional[Dict[str, Any]] = None) -> Any:
    # Some deployments return a GeoJSON FeatureCollection for work zones
    root = _api_root(OHGO_BASE_URL)
    url = _build_url(root, "/work-zones")
    auth = _auth()
    q = {}
    for k in ("region", "map-bounds-sw", "map-bounds-ne", "radius", "page-all", "pageAll"):
        if filters and (k in filters) and (filters[k] is not None):
            q[k] = filters[k]
    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, q, auth["headers"])
    # return raw for GeoJSON
    return data

async def fetch_ohgo_all(
    page_all: bool = True,
    region: Optional[str] = None,
    bounds_sw: Optional[str] = None,
    bounds_ne: Optional[str] = None,
    radius: Optional[str] = None,
    include: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Collects multiple OHGO datasets. By default (include=None) returns ONLY incidents
    to avoid 404s from unknown deployments. Pass include list to fetch extras:
      incidents, construction, digital_signs, cameras, travel_delays,
      dangerous_slowdowns, truck_parking, weather_sensor_sites, work_zones_wzdx
    """
    # Always fetch incidents first
    incidents = await fetch_ohgo_incidents(
        page_all=page_all,
        region=region,
        bounds_sw=bounds_sw,
        bounds_ne=bounds_ne,
        radius=radius,
    )
    out: Dict[str, Any] = {"incidents": incidents}

    # If no extras requested, return now
    if not include:
        log.info("[OHGO] collect (incidents only) count=%d", len(incidents))
        return out

    filters: Dict[str, Any] = {}
    if page_all:
        filters["page-all"] = "true"
    if region:
        filters["region"] = region
    if bounds_sw and bounds_ne:
        filters["map-bounds-sw"] = bounds_sw
        filters["map-bounds-ne"] = bounds_ne
    if radius:
        filters["radius"] = radius

    # Map include token -> function
    fetch_map = {
        "construction": fetch_ohgo_construction,
        "digital_signs": fetch_ohgo_digital_signs,
        "cameras": fetch_ohgo_cameras,
        "travel_delays": fetch_ohgo_travel_delays,
        "dangerous_slowdowns": fetch_ohgo_dangerous_slowdowns,
        "truck_parking": fetch_ohgo_truck_parking,
        "weather_sensor_sites": fetch_ohgo_weather_sensor_sites,
        "work_zones_wzdx": fetch_ohgo_work_zones_wzdx,
    }

    for key in include:
        fn = fetch_map.get(key)
        if not fn:
            log.info("[OHGO] collect skip unknown include=%s", key)
            continue
        try:
            out[key] = await fn(filters)
            if isinstance(out[key], list):
                log.info("[OHGO] collect %s count=%d", key, len(out[key]))
            else:
                log.info("[OHGO] collect %s type=%s", key, type(out[key]).__name__)
        except httpx.HTTPStatusError as e:
            # 404 (endpoint not enabled on this deployment) → return empty
            if e.response is not None and e.response.status_code == 404:
                log.warning("[OHGO] collect %s 404 (omitting)", key)
                out[key] = []
            else:
                log.exception("[OHGO] collect %s error: %s", key, e)
                out[key] = []

    return out
