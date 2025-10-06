#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: scripts/bump_version.py
# Purpose: Bump semantic version and re-apply headers
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
import yaml, subprocess, os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
VERSIONS = os.path.join(ROOT, "versions.yml")
CHANGELOG = os.path.join(ROOT, "CHANGELOG.md")

def bump(part="patch"):
    with open(VERSIONS, "r") as f:
        data = yaml.safe_load(f)
    ver = data.get("version", "0.0.0")
    major, minor, patch = [int(x) for x in ver.split(".")]
    if part == "major":
        major += 1; minor = 0; patch = 0
    elif part == "minor":
        minor += 1; patch = 0
    else:
        patch += 1
    new_ver = f"{major}.{minor}.{patch}"
    data["version"] = new_ver
    with open(VERSIONS, "w") as f:
        yaml.safe_dump(data, f)
    with open(CHANGELOG, "a") as f:
        f.write(f"\n## {new_ver} - {datetime.utcnow().date()}\n- Version bump.\n")
    print(new_ver)

if __name__ == "__main__":
    import sys
    bump(sys.argv[1] if len(sys.argv)>1 else "patch")
