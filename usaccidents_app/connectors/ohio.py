#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/ohio.py
# Purpose: OHGO connector (full fetch, duplicate-safe ingest, collectors)
#
# Description of code and how it works:
# - Uses OHGO documented filters; default is **page-all=true** to fetch the full statewide set.
# - Optional filters via env: OHGO_REGION, OHGO_BOUNDS_SW, OHGO_BOUNDS_NE, OHGO_RADIUS "lat,lon,miles".
# - API Result wrapper parsed **case-insensitively** (handles Results/links/TotalResultCount).
# - Falls back across param styles: kebab-case → camelCase for page-all/page-size/page.
# - Collects link[rel=self] as source_url; maps OHGO “Category/RouteName/RoadStatus” to our fields.
# - Ingest returns detailed counts when requested (inserted/updated/skipped).
# - Incidents: supports page-all + region/bounds/radius filters; tolerant JSON parsing.
# - Ingest: idempotent updates by checking uuid candidates + (source_system, source_event_id).
# - API collectors: fetch cameras, construction, etc., using API-root normalization; skip 404s.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.9.1
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.9.1 (2025-10-08): Rename ingest functions to ingest_ohgo_*; keep backward-compat aliases.
# - 0.9.0 (2025-10-08): Collector hardening (API-root normalize, 404 skip) + /collect support.
# - 0.8.1 (2025-10-08): Duplicate-safe ingest (uuid candidates + composite key); std new uuid 'ohgo:<id>'.
# - 0.8.0 (2025-10-08): Full-fetch + filters + case-insensitive parsing.
# - 0.6.3 (2025-10-07): Clip direction to 32 chars to match widened schema.
###################################################################
#
from typing import Dict, List, Optional, Any
import os
import json
import httpx
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from dotenv import load_dotenv

from ..models import Incident, Road

load_dotenv()

OHGO_API_KEY = os.getenv("OHGO_API_KEY")
OHGO_BASE_URL = os.getenv("OHGO_BASE_URL", "https://publicapi.ohgo.com/api/v1")
OHGO_INCIDENTS_PATH = os.getenv("OHGO_INCIDENTS_PATH", "/incidents")
OHGO_ROADS_PATH = os.getenv("OHGO_ROADS_PATH", "/roads")
OHGO_REGION = os.getenv("OHGO_REGION")
OHGO_BOUNDS_SW = os.getenv("OHGO_BOUNDS_SW")
OHGO_BOUNDS_NE = os.getenv("OHGO_BOUNDS_NE")
OHGO_RADIUS = os.getenv("OHGO_RADIUS")
APP_ENV = (os.getenv("APP_ENV") or "local").lower()


# -------------------------- URL / auth helpers --------------------------

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

def _api_root(base: str) -> str:
    """
    Return the API 'root' (e.g., https://.../api/v1) even if base ends with a resource like /incidents.
    """
    u = (base or "").rstrip("/")
    tail = u.rsplit("/", 1)[-1].lower()
    tails = {
        "incidents", "roads", "construction", "digital-signs", "cameras",
        "travel-delays", "truck-parking", "weather-sensor-sites",
        "dangerous-slowdowns", "work-zones"
    }
    if tail in tails:
        return u.rsplit("/", 1)[0]
    return u

def _auth() -> Dict[str, Dict[str, str]]:
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    if OHGO_API_KEY:
        headers["Authorization"] = f"APIKEY {OHGO_API_KEY}"
        params["api-key"] = OHGO_API_KEY
    return {"headers": headers, "params": params}

async def _request_json(client: httpx.AsyncClient, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Any:
    resp = await client.get(url, params=params, headers=headers or {})
    if resp.status_code in (401, 403) and OHGO_API_KEY:
        qp = dict(params); qp["api-key"] = OHGO_API_KEY
        resp = await client.get(url, params=qp)
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


# -------------------------- JSON helpers --------------------------

def _ci_get(d: Dict[str, Any], key: str, default=None):
    lk = key.lower()
    for k, v in d.items():
        if str(k).lower() == lk:
            return v
    return default

def _as_items(data: Any):
    meta = {}
    if isinstance(data, dict):
        for k in ("Results", "items", "data", "results", "incidents", "content", "value"):
            val = _ci_get(data, k)
            if isinstance(val, list):
                for mk in ("TotalResultCount", "CurrentResultCount", "TotalPageCount", "LastUpdated"):
                    mv = _ci_get(data, mk)
                    if mv is not None:
                        meta[mk] = mv
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

def _clip(val: Optional[str], maxlen: int) -> Optional[str]:
    if val is None:
        return None
    s = str(val)
    return s if len(s) <= maxlen else s[:maxlen]

def _parse_dt(value: Optional[str]):
    if not value:
        return None
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


# -------------------------- Incident normalize/fetch --------------------------

def _normalize_incident(item: Dict[str, Any]) -> Dict[str, Any]:
    props = item.get("properties")
    geom = item.get("geometry")
    if isinstance(props, dict):
        coords = (geom or {}).get("coordinates") if isinstance(geom, dict) else None
        lat = coords[1] if (isinstance(coords, list) and len(coords) > 1) else None
        lon = coords[0] if (isinstance(coords, list) and len(coords) > 0) else None
        route_name = props.get("routeName")
        route_class = "INTERSTATE" if str(route_name or "").startswith("I-") else "STATE"
        return {
            "uuid": item.get("id"),
            "id": item.get("id"),
            "state": props.get("state", "OH"),
            "route": route_name,
            "routeClass": route_class,
            "direction": props.get("direction"),
            "latitude": lat,
            "longitude": lon,
            "reportedTime": props.get("startTime"),
            "updatedTime": props.get("lastUpdated"),
            "clearedTime": props.get("endTime"),
            "isActive": props.get("isActive", True),
            "eventType": props.get("type") or props.get("category"),
            "lanesAffected": props.get("lanesAffected"),
            "closureStatus": props.get("status") or props.get("roadStatus"),
            "severityFlag": props.get("severity"),
            "severityScore": props.get("severityScore"),
            "url": _extract_link(item, "self") or item.get("url"),
        }
    # flat form
    return {
        "uuid": item.get("id"),
        "id": item.get("id"),
        "state": "OH",
        "route": item.get("routeName"),
        "routeClass": ("INTERSTATE" if str(item.get("routeName") or "").startswith("I-") else "STATE"),
        "direction": item.get("direction"),
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "reportedTime": None,
        "updatedTime": None,
        "clearedTime": None,
        "isActive": None,
        "eventType": item.get("category") or item.get("type"),
        "lanesAffected": None,
        "closureStatus": item.get("roadStatus"),
        "severityFlag": None,
        "severityScore": None,
        "url": _extract_link(item, "self") or item.get("url"),
        "location": item.get("location"),
        "description": item.get("description"),
        "roadClosureDetails": item.get("roadClosureDetails"),
    }

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
    elif page_size:
        params["page-size"] = page_size
        params["page"] = 1

    region = (region or OHGO_REGION)
    if region:
        params["region"] = region
    if bounds_sw and bounds_ne:
        params["map-bounds-sw"] = bounds_sw
        params["map-bounds-ne"] = bounds_ne
    elif OHGO_BOUNDS_SW and OHGO_BOUNDS_NE:
        params["map-bounds-sw"] = OHGO_BOUNDS_SW
        params["map-bounds-ne"] = OHGO_BOUNDS_NE
    if radius or OHGO_RADIUS:
        params["radius"] = radius or OHGO_RADIUS

    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, params, auth["headers"])

        if isinstance(data, dict) and ("Error" in data or "error" in data):
            alt = dict(params)
            if "page-all" in alt:
                alt.pop("page-all"); alt["pageAll"] = "true"
            if "page-size" in alt:
                alt["pageSize"] = alt.pop("page-size")
            data = await _request_json(client, url, alt, auth["headers"])

        items, meta = _as_items(data)

        if not page_all and isinstance(data, dict):
            all_items = list(items)
            page_num = params.get("page", 1) if isinstance(params.get("page"), int) else 1
            while True:
                total_pages = (data.get("TotalPageCount") or data.get("totalPages") or 0) if isinstance(data, dict) else 0
                page_num += 1
                if total_pages and page_num > int(total_pages):
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

    if APP_ENV in ("local", "dev", "debug"):
        print(f"[OHGO] fetched {len(out)} incidents from {url}")
    return out


# -------------------------- Duplicate-safe ingest --------------------------

def _uuid_candidates(source_event_id: str, item_uuid: Optional[str]) -> List[str]:
    c = []
    if item_uuid:
        c.append(str(item_uuid))
    if source_event_id:
        c.append(str(source_event_id))
        c.append(f"ohgo:{source_event_id}")
    return list(dict.fromkeys(c))

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
        existing = (
            db.query(Incident)
              .filter(
                  or_(
                      Incident.uuid.in_(candidates),
                      and_(Incident.source_system == "OHGO", Incident.source_event_id == source_event_id),
                  )
              )
              .first()
        )

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
            snapshot = (
                existing.state, existing.route, existing.route_class, existing.direction,
                existing.latitude, existing.longitude, existing.reported_time, existing.updated_time,
                existing.cleared_time, existing.is_active, existing.event_type, existing.lanes_affected,
                existing.closure_status, existing.severity_flag, existing.severity_score
            )
            for k, v in common.items():
                setattr(existing, k, v)
            existing.source_url = item.get("url")
            existing.raw_blob = item
            newshot = (
                existing.state, existing.route, existing.route_class, existing.direction,
                existing.latitude, existing.longitude, existing.reported_time, existing.updated_time,
                existing.cleared_time, existing.is_active, existing.event_type, existing.lanes_affected,
                existing.closure_status, existing.severity_flag, existing.severity_score
            )
            if newshot != snapshot:
                updated += 1
            else:
                skipped += 1
        else:
            new_uuid = f"ohgo:{source_event_id}"
            db.add(Incident(
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
            ))
            inserted += 1

    db.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped} if return_detail else (inserted + updated)

# --- Backward-compat alias (old name) ---
ingest_ohio_incidents = ingest_ohgo_incidents


# -------------------------- Roads --------------------------

async def fetch_ohgo_roads() -> List[Dict[str, Any]]:
    url = _build_url(OHGO_BASE_URL, OHGO_ROADS_PATH)
    auth = _auth()
    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, {}, auth["headers"])
    items, _ = _as_items(data)
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
        count += 1
    db.commit()
    return count

# --- Backward-compat alias (old name) ---
ingest_ohio_roads = ingest_ohgo_roads


# -------------------------- Generic collectors + "all" --------------------------

async def _fetch_items(path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Generic fetcher for non-incident endpoints.
    Always builds from API root (not from /incidents).
    Gracefully returns [] on 404 to avoid crashing the collector.
    """
    base_root = _api_root(OHGO_BASE_URL)
    url = _build_url(base_root, path)
    auth = _auth()
    q = dict(params or {})
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            data = await _request_json(client, url, q, auth["headers"])
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                if APP_ENV in ("local", "dev", "debug"):
                    print(f"[OHGO] 404 for {url} — skipping.")
                return []
            raise
    items, _ = _as_items(data)
    return items

async def fetch_ohgo_construction(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/construction", filters or {})

async def fetch_ohgo_digital_signs(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/digital-signs", filters or {})

async def fetch_ohgo_cameras(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/cameras", filters or {})

async def fetch_ohgo_travel_delays(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/travel-delays", filters or {})

async def fetch_ohgo_dangerous_slowdowns(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/dangerous-slowdowns", filters or {})

async def fetch_ohgo_truck_parking(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/truck-parking", filters or {})

async def fetch_ohgo_weather_sensor_sites(**filters) -> List[Dict[str, Any]]:
    return await _fetch_items("/weather-sensor-sites", filters or {})

async def fetch_ohgo_work_zones_wzdx() -> Dict[str, Any]:
    url = _build_url(_api_root(OHGO_BASE_URL), "/work-zones/wzdx/4.2")
    auth = _auth()
    async with httpx.AsyncClient(timeout=30.0) as client:
        data = await _request_json(client, url, {}, auth["headers"])
    return data if isinstance(data, dict) else {"data": data}

_ALL_KEYS = [
    "incidents", "construction", "digital_signs", "cameras",
    "travel_delays", "dangerous_slowdowns", "truck_parking",
    "weather_sensor_sites", "work_zones_wzdx",
]

async def fetch_ohgo_all(page_all: bool = True,
                         region: Optional[str] = None,
                         bounds_sw: Optional[str] = None,
                         bounds_ne: Optional[str] = None,
                         radius: Optional[str] = None,
                         include: Optional[List[str]] = None) -> Dict[str, Any]:
    use = set(k for k in (include or _ALL_KEYS) if k in _ALL_KEYS)

    common_filters: Dict[str, Any] = {}
    if page_all:
        common_filters["page-all"] = "true"
    if region or OHGO_REGION:
        common_filters["region"] = region or OHGO_REGION
    if bounds_sw and bounds_ne:
        common_filters["map-bounds-sw"] = bounds_sw
        common_filters["map-bounds-ne"] = bounds_ne
    elif OHGO_BOUNDS_SW and OHGO_BOUNDS_NE:
        common_filters["map-bounds-sw"] = OHGO_BOUNDS_SW
        common_filters["map-bounds-ne"] = OHGO_BOUNDS_NE
    if radius or OHGO_RADIUS:
        common_filters["radius"] = radius or OHGO_RADIUS

    out: Dict[str, Any] = {}

    async def safe(name, coro):
        try:
            out[name] = await coro
        except httpx.HTTPStatusError as e:
            code = e.response.status_code if e.response is not None else "?"
            if APP_ENV in ("local", "dev", "debug"):
                print(f"[OHGO] {name} fetch failed with {code}; skipping.")
            out[name] = [] if name != "work_zones_wzdx" else {}

    if "incidents" in use:
        out["incidents"] = await fetch_ohgo_incidents(page_size=100, page_all=True,
                                                      region=region, bounds_sw=bounds_sw,
                                                      bounds_ne=bounds_ne, radius=radius)

    if "construction" in use:
        await safe("construction", fetch_ohgo_construction(**common_filters))
    if "digital_signs" in use:
        await safe("digital_signs", fetch_ohgo_digital_signs(**common_filters))
    if "cameras" in use:
        await safe("cameras", fetch_ohgo_cameras(**common_filters))
    if "travel_delays" in use:
        await safe("travel_delays", fetch_ohgo_travel_delays(**common_filters))
    if "dangerous_slowdowns" in use:
        await safe("dangerous_slowdowns", fetch_ohgo_dangerous_slowdowns(**common_filters))
    if "truck_parking" in use:
        await safe("truck_parking", fetch_ohgo_truck_parking(**common_filters))
    if "weather_sensor_sites" in use:
        await safe("weather_sensor_sites", fetch_ohgo_weather_sensor_sites(**common_filters))
    if "work_zones_wzdx" in use:
        try:
            out["work_zones_wzdx"] = await fetch_ohgo_work_zones_wzdx()
        except httpx.HTTPStatusError:
            if APP_ENV in ("local", "dev", "debug"):
                print(f"[OHGO] work_zones_wzdx fetch failed; skipping.")
            out["work_zones_wzdx"] = {}

    if APP_ENV in ("local", "dev", "debug"):
        summary = {k: (len(v) if isinstance(v, list) else "geojson") for k, v in out.items()}
        print(f"[OHGO] collected: {summary}")
    return out
