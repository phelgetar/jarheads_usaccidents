#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: usaccidents_app/logging_config.py
# Purpose: Centralized logging setup (rotating file, redaction, levels)
#
# Description of code and how it works:
# - Creates a TimedRotatingFileHandler (daily) + console handler.
# - Redacts OHGO_API_KEY if it ever appears in logs.
# - Respects env: USACCIDENTS_LOG_DIR, USACCIDENTS_LOG_FILE, USACCIDENTS_LOG_LEVEL.
#
# Author: Tim Canady
# Created: 2025-10-09
#
# Version: 1.0.0
# Last Modified: 2025-10-09 by Tim Canady
#
# Revision History:
# - 1.0.0 (2025-10-09): Initial logging bundle.
###################################################################
#
from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

class _RedactFilter(logging.Filter):
    def __init__(self, api_key: str | None):
        super().__init__()
        self.api_key = api_key or ""

    def filter(self, record: logging.LogRecord) -> bool:
        if self.api_key:
            # Redact in message
            if isinstance(record.msg, str) and self.api_key in record.msg:
                record.msg = record.msg.replace(self.api_key, "***")
            # Redact in args
            if record.args:
                try:
                    if isinstance(record.args, dict):
                        record.args = {
                            k: ("***" if (isinstance(v, str) and self.api_key in v) else v)
                            for k, v in record.args.items()
                        }
                    elif isinstance(record.args, tuple):
                        record.args = tuple("***" if (isinstance(v, str) and self.api_key in v) else v
                                            for v in record.args)
                except Exception:
                    pass
        return True

def setup_logging() -> logging.Logger:
    # Where to write logs
    project_root = Path(__file__).resolve().parents[1]
    log_dir = Path(os.getenv("USACCIDENTS_LOG_DIR", project_root / "logs"))
    _ensure_dir(log_dir)
    log_file = Path(os.getenv("USACCIDENTS_LOG_FILE", log_dir / "usaccidents_app.log"))

    # Level
    level_name = os.getenv("USACCIDENTS_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Base logger for project
    logger = logging.getLogger("usaccidents")
    logger.setLevel(level)
    logger.propagate = False  # don't double-log to root

    # Formatter
    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )

    # Redact OHGO_API_KEY if present
    api_key = os.getenv("OHGO_API_KEY")
    redact_filter = _RedactFilter(api_key)

    # File handler: rotate at midnight, keep 7 days
    fh = TimedRotatingFileHandler(str(log_file), when="midnight", backupCount=7, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    fh.addFilter(redact_filter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    ch.addFilter(redact_filter)
    logger.addHandler(ch)

    logger.info("Logging initialized at %s (file=%s)", level_name, log_file)
    return logger
