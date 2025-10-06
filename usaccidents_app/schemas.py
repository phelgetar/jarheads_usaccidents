#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/schemas.py
# Purpose: Pydantic models (v1) for API payloads.
#
# Description of code and how it works:
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.6.0
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.6.0 (2025-10-04): Ensure `get_db` generator and explicit exports; robust env loading.
# - 0.5.0 (2025-09-28): MySQL engine options / pool_pre_ping.
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.3.2 (2025-09-28): Connector refactor + DB tuning
# - 0.1.0 (2025-09-22): Initial DB bootstrap.
###################################################################
#
from typing import Optional, Any, List
from pydantic import BaseModel

class IncidentBase(BaseModel):
    uuid: str
    source_system: str
    source_event_id: Optional[str]
    source_url: Optional[str]
    state: Optional[str]
    county: Optional[str]
    route: Optional[str]
    route_class: Optional[str]
    direction: Optional[str]
    milepost: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]
    reported_time: Optional[str]
    updated_time: Optional[str]
    cleared_time: Optional[str]
    is_active: Optional[bool]
    event_type: Optional[str]
    lanes_affected: Optional[str]
    closure_status: Optional[str]
    severity_flag: Optional[str]
    severity_score: Optional[float]
    units_involved: Optional[int]
    count: Optional[int]
    image_urls_allowed: Optional[Any]
    article_urls: Optional[Any]
    raw_blob: Optional[Any]

class IncidentOut(IncidentBase):
    id: int
    class Config:
        orm_mode = True

class RoadBase(BaseModel):
    source_system: Optional[str]
    road_id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    direction: Optional[str]
    begin_mile: Optional[float]
    end_mile: Optional[float]
    length: Optional[float]
    geometry: Optional[Any]
    last_updated: Optional[str]

class RoadOut(RoadBase):
    id: int
    class Config:
        orm_mode = True
