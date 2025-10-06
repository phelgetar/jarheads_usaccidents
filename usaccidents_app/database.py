#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/database.py
# Purpose: SQLAlchemy engine/session factory.
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
from typing import Generator
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set in environment or .env")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
