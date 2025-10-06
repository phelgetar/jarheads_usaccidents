#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/models.py
# Purpose: ORM models for incidents and roads.
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
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, UniqueConstraint, Index
from .database import Base

class Incident(Base):
    __tablename__ = "incidents"
    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String(64), nullable=False, unique=True, index=True)
    source_system = Column(String(64), nullable=False, index=True)
    source_event_id = Column(String(128), nullable=True, index=True)
    source_url = Column(String(length=65535), nullable=True)
    state = Column(String(2), nullable=True, index=True)
    county = Column(String(128), nullable=True)
    route = Column(String(64), nullable=True, index=True)
    route_class = Column(String(32), nullable=True, index=True)
    direction = Column(String(8), nullable=True)
    milepost = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True, index=True)
    longitude = Column(Float, nullable=True, index=True)
    reported_time = Column(DateTime, nullable=True, index=True)
    updated_time = Column(DateTime, nullable=True)
    cleared_time = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=True)
    event_type = Column(String(64), nullable=True)
    lanes_affected = Column(String(64), nullable=True)
    closure_status = Column(String(32), nullable=True)
    severity_flag = Column(String(16), nullable=True)
    severity_score = Column(Float, nullable=True)
    units_involved = Column(Integer, nullable=True)
    count = Column(Integer, nullable=True)
    image_urls_allowed = Column(JSON, nullable=True)
    article_urls = Column(JSON, nullable=True)
    raw_blob = Column(JSON, nullable=True)
    duplicate_of = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("uuid", name="ix_incidents_uuid"),
        Index("idx_state_route", "state", "route"),
        Index("idx_reported_time", "reported_time"),
        Index("idx_updated_time", "updated_time"),
    )

class Road(Base):
    __tablename__ = "roads"
    id = Column(Integer, primary_key=True, index=True)
    source_system = Column(String(50), nullable=True, index=True)
    road_id = Column(String(50), nullable=True, index=True)
    name = Column(String(100), nullable=True)
    description = Column(String(length=65535), nullable=True)
    direction = Column(String(20), nullable=True)
    begin_mile = Column(Float, nullable=True)
    end_mile = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    geometry = Column(JSON, nullable=True)
    last_updated = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("source_system", "road_id", name="uq_road_source_id"),
    )
