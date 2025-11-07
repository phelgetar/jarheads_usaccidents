#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/connectors/texas.py
# Purpose: DriveTexas (TxDOT) connector - conditions GeoJSON
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
# Highlights
# - Pulls https://api.drivetexas.org/api/conditions.geojson
# - Auth with ?key=DRIVETEXAS_API_KEY (env)
# - Normalizes GeoJSON features -> our incident schema (with times)
# - Derives is_active, closure_status, severity when absent
# - Duplicate-safe ingest (per-item commit + IntegrityError upsert)
# - Structured logging via "usaccidents" logger
#
# Env:
#   DRIVETEXAS_API_KEY (required)
#   DRIVETEXAS_BASE    (default: https://api.drivetexas.org)
#   DRIVETEXAS_PATH    (default: /api/conditions.geojson)
#
# Revision History:
# - 0.1.0 (2025-10-14): Initial DriveTexas fetch/ingest implementation.
###################################################################
from __future__ import annotations
from typing import Dict, List, Optional, Any, Tuple
import os, re, html, logging, json
from datetime import datetime, timezone
import httpx
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
from sqlalchemy.exc import IntegrityError

from ..models import Incident

log = logging.getLogger("usaccidents")

# --------- Env / Defaults -----------------------------------------------------

DRIVETEXAS_API_KEY = os.getenv("DRIVETEXAS_API_KEY")
DRIVETEXAS_BASE = os.getenv("DRIVETEXAS_BASE", "https://api.drivetexas.org")
DRIVETEXAS_PATH = os.getenv("DRIVETEXAS_PATH", "/api/conditions.geojson")

# Backward-compat for old variable name the user tried:
_DRIVETEXAS_BASE_URL = os.getenv("DRIVETEXAS_BASE_URL")
if _DRIVETEXAS_BASE_URL and _DRIVETEXAS_BASE_URL.endswith(".geojson"):
    # allow full URL via BASE_URL if provided correctly
    DRIVETEXAS_BASE = _DRIVETEXAS_BASE_URL.rsplit("/api", 1)[0]
    DRIVETEXAS_PATH = "/api" + _DRIVETEXAS_BASE_URL.rsplit("/api", 1)[1]

_HTML_TAG_RE = re.compile(r"<[^>]+>")

# --------- Small utils --------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601 with offset (e.g., 2025-10-20T21:00:00-05:00 or Z)."""
    if not value:
        return None
    v = str(value)
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None

def _clip(s: Optional[str], n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n]

def _strip_html(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    # unescape & strip simple tags
    t = html.unescape(s)
    t = t.replace("<br/>", " ").replace("<br>", " ").replace("<br />", " ")
    return _HTML_TAG_RE.sub("", t).strip()

def _first_lonlat(geom: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Extract a representative (lon, lat) from Point/LineString/Multi* geometry."""
    if not isinstance(geom, dict):
        return (None, None)
    gtype = (geom.get("type") or "").lower()
    coords = geom.get("coordinates")
    if not coords:
        return (None, None)

    def _pair(v):
        return (v[0], v[1]) if isinstance(v, list) and len(v) >= 2 else (None, None)

    if gtype == "point":
        return _pair(coords)
    if gtype == "linestring":
        if isinstance(coords, list) and coords:
            return _pair(coords[0])
    if gtype == "multilinestring":
        # [[ [lon,lat], ... ], [ ... ]]
        if isinstance(coords, list) and coords and isinstance(coords[0], list) and coords[0]:
            return _pair(coords[0][0])
    if gtype == "multipoint":
        if isinstance(coords, list) and coords:
            return _pair(coords[0])
    if gtype == "polygon":
        # first ring, first vertex
        if isinstance(coords, list) and coords and isinstance(coords[0], list) and coords[0]:
            return _pair(coords[0][0])

    # fallback best effort
    try:
        flat = coords
        while isinstance(flat, list) and flat and isinstance(flat[0], list):
            flat = flat[0]
        return _pair(flat)
    except Exception:
        return (None, None)

def _derive_active(end_time: Optional[datetime]) -> int:
    # active if no end_time, or end_time in the future
    if end_time is None:
        return 1
    return 1 if end_time > _utcnow() else 0

def _status_from_condition_desc(condition: Optional[str], description: Optional[str]) -> str:
    c = (condition or "").lower()
    d = (description or "").lower()
    if "closure" in c or "closed" in c or "closure" in d or "closed" in d:
        return "CLOSED"
    if any(k in d for k in ("lane closed", "alternating lanes", "shoulder closed", "lane shift", "one lane", "reduced")):
        return "PARTIAL"
    if "open" in d:
        return "OPEN"
    return "UNKNOWN"

def _severity_from_condition(condition: Optional[str], delay_flag: Optional[str | bool]) -> Tuple[Optional[str], Optional[int]]:
    # delay_flag may be "true"/"false" or boolean
    if isinstance(delay_flag, str):
        delay_flag = delay_flag.strip().lower() == "true"
    if delay_flag:
        return ("Delay", 2)
    c = (condition or "").lower()
    if any(k in c for k in ("closure", "crash", "incident", "accident")):
        return ("HIGH", 3)
    if any(k in c for k in ("construction", "work", "maintenance")):
        return ("MEDIUM", 2)
    return ("LOW", 1) if c else (None, None)

# --------- Normalization ------------------------------------------------------

def _normalize_feature(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Input: GeoJSON Feature with .properties and .geometry
    Output: dict ready for ingest mapping
    """
    if not isinstance(feat, dict):
        return None
    props = feat.get("properties") or {}
    geom = feat.get("geometry") or {}

    lon, lat = _first_lonlat(geom)

    src_id = props.get("GLOBALID") or props.get("Identifier") or props.get("id")
    if not src_id:
        return None

    route = props.get("route_name") or props.get("roadwayName") or props.get("Route")
    direction = props.get("travel_direction") or props.get("direction")
    condition = props.get("condition") or props.get("type")
    description = _strip_html(props.get("description"))

    start_time_s = props.get("start_time") or props.get("create_time")
    end_time_s = props.get("end_time")
    start_time = _parse_dt(start_time_s)
    end_time = _parse_dt(end_time_s)
    updated_time = start_time or _utcnow()  # DriveTexas has no per-incident lastUpdated

    delay_flag = props.get("delay_flag")
    county = props.get("county_num") or props.get("county")

    closure_status = _status_from_condition_desc(condition, description)
    severity_flag, severity_score = _severity_from_condition(condition, delay_flag)
    is_active = _derive_active(end_time)

    # route class heuristic
    route_class = "INTERSTATE" if str(route or "").startswith(("IH", "I-", "IH0", "IH00")) else "STATE"

    return {
        # identity
        "id": str(src_id),
        "uuid": f"tx:{src_id}",
        "source_url": None,

        # core location
        "state": "TX",
        "route": route,
        "routeClass": route_class,
        "direction": _clip(direction, 32),
        "latitude": lat,
        "longitude": lon,

        # timing (strings to parse later)
        "reportedTime": start_time_s,
        "updatedTime": start_time_s,
        "clearedTime": end_time_s,

        # status/meta
        "isActive": bool(is_active),
        "eventType": condition,
        "lanesAffected": None,  # not explicit in feed
        "closureStatus": closure_status,
        "severityFlag": severity_flag,
        "severityScore": severity_score,

        # extras
        "county": county,
        "location": props.get("location"),
        "description": description,
        "raw_props": props,
    }

# --------- Fetch --------------------------------------------------------------

async def fetch_texas_incidents() -> List[Dict[str, Any]]:
    """
    Fetch the DriveTexas conditions feed and normalize to incident-like dicts.
    """
    if not DRIVETEXAS_API_KEY:
        log.error("[DRIVETEXAS] DRIVETEXAS_API_KEY is not set")
        return []

    base = DRIVETEXAS_BASE.rstrip("/")
    path = "/" + DRIVETEXAS_PATH.lstrip("/")
    url = f"{base}{path}"

    params = {"key": DRIVETEXAS_API_KEY}
    headers = {"Accept": "application/json", "User-Agent": "USAccidents/1.0"}

    log.info("[DRIVETEXAS] fetch start url=%s", url)
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(url, params=params, headers=headers)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            log.error("[DRIVETEXAS] HTTP %s body[:300]=%r", r.status_code, r.text[:300])
            return []

        ct = r.headers.get("content-type", "")
        if "json" not in ct.lower():
            # still try JSON decode; if fails, bail
            log.warning("[DRIVETEXAS] unusual content-type: %s", ct)

        try:
            data = r.json()
        except ValueError:
            log.error("[DRIVETEXAS] JSON parse failed body[:200]=%r", r.text[:200])
            return []

    feats = data.get("features") if isinstance(data, dict) else None
    if not isinstance(feats, list):
        log.warning("[DRIVETEXAS] unexpected response shape (no features array)")
        return []

    out: List[Dict[str, Any]] = []
    for f in feats:
        norm = _normalize_feature(f)
        if norm:
            out.append(norm)

    log.info("[DRIVETEXAS] fetch done count=%d", len(out))
    return out

# --------- Ingest (duplicate-safe) -------------------------------------------

def _uuid_candidates(source_event_id: str, item_uuid: Optional[str]) -> List[str]:
    c: List[str] = []
    if item_uuid:
        c.append(str(item_uuid))
    if source_event_id:
        c.append(str(source_event_id))
        c.append(f"tx:{source_event_id}")
    return list(dict.fromkeys(c))

def _apply_common(existing: Incident, common: Dict[str, Any], raw: Dict[str, Any]):
    for k, v in common.items():
        setattr(existing, k, v)
    existing.source_url = raw.get("source_url")
    existing.raw_blob = raw

def ingest_texas_incidents(db: Session, items: List[Dict[str, Any]], return_detail: bool = False):
    """
    Insert/update DriveTexas items into incidents table.
    - source_system = 'TXDOT'
    - uuid format for new rows: 'tx:<id>'
    - reported_time preserved once set (first-seen), updated_time always refreshed
    """
    inserted = updated = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        source_event_id = str(item.get("id") or "").strip()
        if not source_event_id:
            skipped += 1
            continue

        candidates = _uuid_candidates(source_event_id, item.get("uuid"))
        existing = (
            db.query(Incident)
            .filter(
                or_(
                    Incident.uuid.in_(candidates),
                    and_(Incident.source_system == "TXDOT", Incident.source_event_id == source_event_id),
                )
            )
            .first()
        )

        # Parse times
        reported_time = _parse_dt(item.get("reportedTime"))
        updated_time = _parse_dt(item.get("updatedTime")) or reported_time or _utcnow()
        cleared_time = _parse_dt(item.get("clearedTime"))

        # Preserve first-seen reported_time if present
        if existing and existing.reported_time and reported_time is None:
            reported_time = existing.reported_time
        if not existing and reported_time is None:
            reported_time = updated_time

        common = dict(
            source_system="TXDOT",
            source_event_id=source_event_id,
            state=item.get("state") or "TX",
            route=item.get("route"),
            route_class=item.get("routeClass"),
            direction=_clip(item.get("direction"), 32),
            latitude=item.get("latitude"),
            longitude=item.get("longitude"),
            reported_time=reported_time,
            updated_time=updated_time,
            cleared_time=cleared_time,
            is_active=1 if item.get("isActive") else 0,
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
            new_uuid = f"tx:{source_event_id}"
            row = Incident(
                uuid=new_uuid,
                source_system=common["source_system"],
                source_event_id=common["source_event_id"],
                source_url=item.get("source_url"),
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
                # race: try update
                existing2 = (
                    db.query(Incident)
                    .filter(
                        or_(
                            Incident.uuid.in_(candidates + [new_uuid]),
                            and_(Incident.source_system == "TXDOT", Incident.source_event_id == source_event_id),
                        )
                    )
                    .first()
                )
                if existing2:
                    _apply_common(existing2, common, item)
                    db.commit()
                    updated += 1
                else:
                    skipped += 1

    if return_detail:
        log.info("[DRIVETEXAS] ingest result inserted=%d updated=%d skipped=%d", inserted, updated, skipped)
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    else:
        log.info("[DRIVETEXAS] ingest result processed=%d", inserted + updated)
        return inserted + updated
