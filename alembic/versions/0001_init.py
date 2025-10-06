#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: alembic/versions/0001_init.py
# Purpose: Ensure indexes and unique constraints.
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
import sqlalchemy as sa

revision = "0001_init"
down_revision = "0000_bootstrap_from_report"
branch_labels = None
depends_on = None

def upgrade():
    try:
        op.create_index("idx_state_route", "incidents", ["state","route"], unique=False)
    except Exception: pass
    try:
        op.create_index("idx_reported_time", "incidents", ["reported_time"], unique=False)
    except Exception: pass
    try:
        op.create_index("idx_updated_time", "incidents", ["updated_time"], unique=False)
    except Exception: pass
    try:
        op.create_unique_constraint("ix_incidents_uuid", "incidents", ["uuid"])
    except Exception: pass

def downgrade():
    try:
        op.drop_constraint("ix_incidents_uuid","incidents", type_="unique")
    except Exception: pass
    for idx in ["idx_updated_time","idx_reported_time","idx_state_route"]:
        try:
            op.drop_index(idx, table_name="incidents")
        except Exception: pass
