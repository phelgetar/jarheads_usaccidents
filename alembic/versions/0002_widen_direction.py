#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: alembic/versions/0002_widen_direction.py
# Purpose: Widen incidents.direction VARCHAR(8) -> VARCHAR(32)
#
# Description of code and how it works:
# - Alters column length to accept values like 'Both Directions'.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.6.3
# Last Modified: 2025-10-07 by Tim Canady
#
# Revision History:
# - 0.6.3 (2025-10-07): Initial migration for direction width.
###################################################################
#
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_widen_direction"
down_revision = "0001_init"
branch_labels = None
depends_on = None

def upgrade():
    try:
        op.alter_column(
            "incidents",
            "direction",
            existing_type=sa.String(length=8),
            type_=sa.String(length=32),
            existing_nullable=True,
        )
    except Exception:
        # Fallback raw SQL for MySQL
        op.execute("ALTER TABLE incidents MODIFY COLUMN direction VARCHAR(32) NULL")

def downgrade():
    try:
        op.alter_column(
            "incidents",
            "direction",
            existing_type=sa.String(length=32),
            type_=sa.String(length=8),
            existing_nullable=True,
        )
    except Exception:
        op.execute("ALTER TABLE incidents MODIFY COLUMN direction VARCHAR(8) NULL")
