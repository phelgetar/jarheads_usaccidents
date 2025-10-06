#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: alembic/versions/0000_bootstrap_from_report.py
# Purpose: Bootstrap tables/indexes based on live schema report.
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
from alembic import op

revision = "0000_bootstrap_from_report"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS incidents (
            id INT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(64) NOT NULL,
            source_system VARCHAR(64) NOT NULL,
            source_event_id VARCHAR(128) NULL,
            source_url TEXT NULL,
            state VARCHAR(2) NULL,
            county VARCHAR(128) NULL,
            route VARCHAR(64) NULL,
            route_class VARCHAR(32) NULL,
            direction VARCHAR(8) NULL,
            milepost DOUBLE NULL,
            latitude DOUBLE NULL,
            longitude DOUBLE NULL,
            reported_time DATETIME NULL,
            updated_time DATETIME NULL,
            cleared_time DATETIME NULL,
            is_active TINYINT(1) NULL,
            event_type VARCHAR(64) NULL,
            lanes_affected VARCHAR(64) NULL,
            closure_status VARCHAR(32) NULL,
            severity_flag VARCHAR(16) NULL,
            severity_score DOUBLE NULL,
            units_involved INT NULL,
            count INT NULL,
            image_urls_allowed JSON NULL,
            article_urls JSON NULL,
            raw_blob JSON NULL,
            duplicate_of INT NULL,
            created_at DATETIME NULL,
            updated_at DATETIME NULL,
            UNIQUE KEY ix_incidents_uuid (uuid),
            KEY idx_state_route (state, route),
            KEY idx_reported_time (reported_time),
            KEY idx_updated_time (updated_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS roads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source_system VARCHAR(50) NULL,
            road_id VARCHAR(50) NULL,
            name VARCHAR(100) NULL,
            description TEXT NULL,
            direction VARCHAR(20) NULL,
            begin_mile DOUBLE NULL,
            end_mile DOUBLE NULL,
            length DOUBLE NULL,
            geometry JSON NULL,
            last_updated DATETIME NULL,
            UNIQUE KEY uq_road_source_id (source_system, road_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """
    )

def downgrade():
    pass
