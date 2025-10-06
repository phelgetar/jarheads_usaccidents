#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/init_versions.py
# Purpose: FastAPI usaccidents_app to ingest, normalize, and serve incidents.
#
# Author: Tim Canady
# Created: 2025-10-04
#
# Version: 0.4.1
# Last Modified: 2025-10-04 by Tim Canady
#
# Revision History:
# - 0.4.1 (2025-10-04): Auto-synced update — Tim Canady
# - 0.4.0 (2025-10-04): Initial version — Tim Canady
###################################################################
#
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
###################################################################
# Project: USAccidents
# File: scripts/init_versions.py
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Initialize versions.yml by scanning for files and seeding 0.1.0.
#
# Version: 0.4.1
#
# Revision History
# - 0.1.1 (2025-09-30): Add extension filter and skip .git — Tim Canady
# - 0.1.0 (2025-09-28): Initial — Tim Canady
###################################################################
#
import pathlib
import sys
import datetime
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
VERS_PATH = REPO_ROOT / "versions.yml"

DEFAULT_PROJECT_VERSION = "0.2.0"
DEFAULT_FILE_VERSION = "0.1.0"
INCLUDE_EXT = {".py", ".sh"}
EXCLUDE_DIRS = {".git", ".idea", "__pycache__", "venv", ".venv", "node_modules", "dist", "build", ".mypy_cache", ".pytest_cache"}

def iter_files():
    for p in REPO_ROOT.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in INCLUDE_EXT:
            continue
        parts = set(p.relative_to(REPO_ROOT).parts)
        if parts & EXCLUDE_DIRS:
            continue
        yield p

def load_versions():
    if VERS_PATH.exists():
        with open(VERS_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

def write_versions(data):
    with open(VERS_PATH, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def main():
    data = load_versions()
    if "project_version" not in data:
        data["project_version"] = DEFAULT_PROJECT_VERSION
    data.setdefault("files", {})
    files = data["files"]

    count_new = 0
    for p in iter_files():
        rel = str(p.relative_to(REPO_ROOT))
        if rel not in files:
            files[rel] = DEFAULT_FILE_VERSION
            count_new += 1

    write_versions(data)
    print(f"versions.yml updated. project_version={data['project_version']}  new_files_added={count_new}")
    print(f"Total tracked files: {len(files)}")

if __name__ == "__main__":
    main()