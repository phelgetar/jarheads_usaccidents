#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: alembic/env.py
# Purpose: Alembic env that loads DATABASE_URL from .env and escapes % for ConfigParser.
#
# Description of code and how it works:
# - Loads .env explicitly from project root to avoid stdin/stack issues.
# - Percent-escapes '%' to '%%' before setting sqlalchemy.url to satisfy ConfigParser.
# - Imports models' Base.metadata for autogeneration.
#
# Author: Tim Canady
# Created: 2025-09-28
#
# Version: 0.7.2
# Last Modified: 2025-10-08 by Tim Canady
#
# Revision History:
# - 0.7.2 (2025-10-08): Header added to satisfy pre-commit — Tim Canady
# - 0.7.2 (2025-10-08): Add standardized header; dotenv explicit path; percent-escape URL.
# - 0.7.1 (2025-10-07): Dotenv-safe loader and root path resolution.
###################################################################
#
from logging.config import fileConfig
import os
from pathlib import Path
from alembic import context
from sqlalchemy import engine_from_config, pool
from dotenv import load_dotenv

# Resolve project root (alembic/env.py -> project root)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load .env explicitly
load_dotenv(PROJECT_ROOT / ".env")

config = context.config

# Escape % to avoid ConfigParser interpolation errors
db_url = os.getenv("DATABASE_URL")
if db_url:
    config.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
else:
    print("[alembic] WARNING: DATABASE_URL not set; using placeholder from alembic.ini")

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import metadata for autogenerate
try:
    from usaccidents_app.models import Base
    target_metadata = Base.metadata
except Exception as ex:
    target_metadata = None
    print(f"[alembic] WARNING: Could not import usaccidents_app.models.Base: {ex}")

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
