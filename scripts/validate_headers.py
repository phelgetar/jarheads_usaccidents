#!/usr/bin/env python3
#
###################################################################
# Project: USAccidents
# File: scripts/validate_headers.py
# Purpose: Validate presence of standardized headers
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
import sys, re

SHELL_RE = re.compile(r"\A#!/usr/bin/env bash\n#\n###################################################################\n# Project: USAccidents", re.M)
PY_RE = re.compile(r"\A#!/usr/bin/env python3\n#\n###################################################################\n# Project: USAccidents", re.M)

def ok(path):
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            txt = f.read()
    except Exception:
        return False
    if path.endswith('.sh'):
        return bool(SHELL_RE.match(txt))
    if path.endswith('.py'):
        return bool(PY_RE.match(txt))
    return True

def main():
    failed = [p for p in sys.argv[1:] if (p.endswith('.py') or p.endswith('.sh')) and not ok(p)]
    if failed:
        print("Header validation failed for:")
        for p in failed:
            print(" -", p)
        sys.exit(1)

if __name__ == "__main__":
    main()
