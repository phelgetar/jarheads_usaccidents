#
###################################################################
# Project: USAccidents Ingestor MVP
# File: scripts/safe_header_apply.py
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
# File: scripts/safe_header_apply.py
#
# Author: Tim Canady
# Date: Sep 2025
#
# Description:
#   Convenience wrapper: run apply_headers.py in dry-run, show plan,
#   then run for real if approved (or --yes).
#
# Version: 0.4.1
#
# Revision History
# - 0.1.0 (2025-09-30): Initial — Tim Canady
###################################################################
#

import argparse
import subprocess
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("paths", nargs="*", default=["usaccidents_app","scripts"])
    p.add_argument("--ext", nargs="+", default=[".py"])
    p.add_argument("--yes", action="store_true", help="Apply without prompt")
    p.add_argument("--date", default=None)
    p.add_argument("--version", default=None)
    p.add_argument("--author", default=None)
    args = p.parse_args()

    base = ["python", "scripts/apply_headers.py"] + args.paths + ["--ext"] + args.ext
    if args.date:    base += ["--date", args.date]
    if args.version: base += ["--version", args.version]
    if args.author:  base += ["--author", args.author]

    print("=== DRY RUN ===")
    subprocess.run(base + ["--dry-run"], check=True)
    if not args.yes:
        resp = input("Apply changes? [y/N] ").strip().lower()
        if resp != "y":
            print("Aborted.")
            sys.exit(0)
    print("=== APPLYING ===")
    subprocess.run(base, check=True)

if __name__ == "__main__":
    main()
