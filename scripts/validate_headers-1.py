#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/validate_headers.py
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

import subprocess
import sys
import re

REQUIRED_LINES = [
    "# Project:",
    "# File:",
    "# Version:",
    "# Last Modified:",
    "# Revision History:",
]


def get_staged_files():
    """Return a list of staged Python or Shell files."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"], capture_output=True, text=True
    )
    files = result.stdout.strip().split("\n")
    return [f for f in files if f.endswith((".py", ".sh"))]


def validate_file(path: str) -> list[str]:
    """Return list of missing header lines for a file."""
    missing = []
    try:
        with open(path, "r") as f:
            content = f.read()
    except Exception:
        return ["(unreadable)"]

    for line in REQUIRED_LINES:
        if line not in content:
            missing.append(line)
    return missing


def main():
    files = get_staged_files()
    if not files:
        sys.exit(0)

    errors = {}
    for f in files:
        missing = validate_file(f)
        if missing:
            errors[f] = missing

    if errors:
        print("❌ Header validation failed.\n")
        for f, missing in errors.items():
            print(f"File: {f}")
            for m in missing:
                print(f"  - Missing: {m}")
            print()
        sys.exit(1)

    print("✅ All staged files contain valid headers.")


if __name__ == "__main__":
    main()